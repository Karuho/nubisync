//! Synchronization planning primitives.

#![forbid(unsafe_code)]

pub use nubisync_core::SyncMode;
use nubisync_core::{RemoteChange, RemoteItem, RemoteItemKind};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootChangeMembership {
    Root,
    Descendant,
    Outside,
    UnresolvedDelete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootCatalogAction {
    Ignore,
    RevalidateRoot,
    UpsertItem,
    DeleteSubtree,
    HydrateSubtree,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootCatalogPlanError {
    InvalidChangeContext,
}

/// Plans one provider change against one selected remote subtree.
///
/// This planner is deliberately pure: it performs no provider request, storage
/// mutation, cursor advancement, or filesystem operation.
///
/// `previously_cataloged` means the changed remote ID already exists in this
/// sync root's authoritative catalog before the change is applied.
///
/// A newly observed folder inside the subtree returns `HydrateSubtree`; callers
/// must fully hydrate that folder and its descendants before committing the
/// change batch or advancing the provider cursor.
pub fn plan_root_catalog_change(
    change: &RemoteChange,
    membership: RootChangeMembership,
    previously_cataloged: bool,
) -> Result<RootCatalogAction, RootCatalogPlanError> {
    match change {
        RemoteChange::Delete { .. } => match membership {
            RootChangeMembership::Root => Ok(RootCatalogAction::RevalidateRoot),
            RootChangeMembership::UnresolvedDelete => {
                if previously_cataloged {
                    Ok(RootCatalogAction::DeleteSubtree)
                } else {
                    Ok(RootCatalogAction::Ignore)
                }
            }
            RootChangeMembership::Descendant | RootChangeMembership::Outside => {
                Err(RootCatalogPlanError::InvalidChangeContext)
            }
        },
        RemoteChange::Upsert(item) => match membership {
            RootChangeMembership::UnresolvedDelete => {
                Err(RootCatalogPlanError::InvalidChangeContext)
            }
            RootChangeMembership::Root => Ok(RootCatalogAction::RevalidateRoot),
            RootChangeMembership::Outside => {
                if previously_cataloged {
                    Ok(RootCatalogAction::DeleteSubtree)
                } else {
                    Ok(RootCatalogAction::Ignore)
                }
            }
            RootChangeMembership::Descendant => {
                if item.trashed {
                    if previously_cataloged {
                        return Ok(RootCatalogAction::DeleteSubtree);
                    }
                    return Ok(RootCatalogAction::Ignore);
                }

                match item.kind {
                    RemoteItemKind::File => Ok(RootCatalogAction::UpsertItem),
                    RemoteItemKind::Folder if previously_cataloged => {
                        Ok(RootCatalogAction::UpsertItem)
                    }
                    RemoteItemKind::Folder => Ok(RootCatalogAction::HydrateSubtree),
                }
            }
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootCatalogMutationPlan {
    Upsert(RemoteItem),
    DeleteSubtree { remote_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootCatalogResolution {
    Noop,
    RevalidateRoot,
    Mutations(Vec<RootCatalogMutationPlan>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootCatalogProjectionError {
    Plan(RootCatalogPlanError),
    InvalidRootIdentifier,
    InvalidInitialCatalogItem,
    DuplicateInitialRemoteId,
    IncompleteInitialCatalog,
    HydrationRequired,
    UnexpectedHydration,
    InvalidHydrationRoot,
    InvalidHydrationItem,
    DuplicateHydrationRemoteId,
    IncompleteHydration,
    IncompleteProjectedCatalog,
}

pub struct RootCatalogProjection {
    root_remote_id: String,
    parents: HashMap<String, Option<String>>,
    children_by_parent: HashMap<String, HashSet<String>>,
}

impl RootCatalogProjection {
    pub fn new(
        root_remote_id: impl Into<String>,
        items: &[RemoteItem],
    ) -> Result<Self, RootCatalogProjectionError> {
        let root_remote_id = root_remote_id.into();
        if root_remote_id.trim().is_empty() {
            return Err(RootCatalogProjectionError::InvalidRootIdentifier);
        }

        let mut projection = Self {
            root_remote_id,
            parents: HashMap::with_capacity(items.len()),
            children_by_parent: HashMap::new(),
        };

        for item in items {
            if item.remote_id.trim().is_empty()
                || item.name.is_empty()
                || item.trashed
                || item.remote_id == projection.root_remote_id
            {
                return Err(RootCatalogProjectionError::InvalidInitialCatalogItem);
            }

            if projection.parents.contains_key(&item.remote_id) {
                return Err(RootCatalogProjectionError::DuplicateInitialRemoteId);
            }

            projection.apply_upsert(item);
        }

        if !projection.is_complete() {
            return Err(RootCatalogProjectionError::IncompleteInitialCatalog);
        }

        Ok(projection)
    }

    pub fn item_count(&self) -> usize {
        self.parents.len()
    }

    pub fn contains(&self, remote_id: &str) -> bool {
        self.parents.contains_key(remote_id)
    }

    pub fn apply_change(
        &mut self,
        change: &RemoteChange,
        membership: RootChangeMembership,
        hydration: Option<Vec<RemoteItem>>,
    ) -> Result<RootCatalogResolution, RootCatalogProjectionError> {
        let remote_id = match change {
            RemoteChange::Delete { remote_id } => remote_id.as_str(),
            RemoteChange::Upsert(item) => item.remote_id.as_str(),
        };
        let previously_cataloged = self.contains(remote_id);

        let action = plan_root_catalog_change(change, membership, previously_cataloged)
            .map_err(RootCatalogProjectionError::Plan)?;

        match action {
            RootCatalogAction::Ignore => {
                reject_unexpected_hydration(hydration)?;
                Ok(RootCatalogResolution::Noop)
            }
            RootCatalogAction::RevalidateRoot => {
                reject_unexpected_hydration(hydration)?;
                Ok(RootCatalogResolution::RevalidateRoot)
            }
            RootCatalogAction::UpsertItem => {
                reject_unexpected_hydration(hydration)?;

                let RemoteChange::Upsert(item) = change else {
                    return Err(RootCatalogProjectionError::Plan(
                        RootCatalogPlanError::InvalidChangeContext,
                    ));
                };

                self.apply_upsert(item);

                Ok(RootCatalogResolution::Mutations(vec![
                    RootCatalogMutationPlan::Upsert(item.clone()),
                ]))
            }
            RootCatalogAction::DeleteSubtree => {
                reject_unexpected_hydration(hydration)?;
                self.delete_subtree(remote_id);

                Ok(RootCatalogResolution::Mutations(vec![
                    RootCatalogMutationPlan::DeleteSubtree {
                        remote_id: remote_id.to_owned(),
                    },
                ]))
            }
            RootCatalogAction::HydrateSubtree => {
                let hydration = hydration.ok_or(RootCatalogProjectionError::HydrationRequired)?;

                let RemoteChange::Upsert(root_item) = change else {
                    return Err(RootCatalogProjectionError::Plan(
                        RootCatalogPlanError::InvalidChangeContext,
                    ));
                };

                validate_hydration(root_item, &hydration)?;

                let mut mutations = Vec::with_capacity(hydration.len());
                for item in hydration {
                    self.apply_upsert(&item);
                    mutations.push(RootCatalogMutationPlan::Upsert(item));
                }

                Ok(RootCatalogResolution::Mutations(mutations))
            }
        }
    }

    pub fn validate_complete(&self) -> Result<(), RootCatalogProjectionError> {
        if self.is_complete() {
            Ok(())
        } else {
            Err(RootCatalogProjectionError::IncompleteProjectedCatalog)
        }
    }

    fn apply_upsert(&mut self, item: &RemoteItem) {
        if let Some(Some(old_parent)) = self.parents.get(&item.remote_id).cloned()
            && let Some(children) = self.children_by_parent.get_mut(&old_parent)
        {
            children.remove(&item.remote_id);
            if children.is_empty() {
                self.children_by_parent.remove(&old_parent);
            }
        }

        self.parents
            .insert(item.remote_id.clone(), item.parent_remote_id.clone());

        if let Some(parent_remote_id) = &item.parent_remote_id {
            self.children_by_parent
                .entry(parent_remote_id.clone())
                .or_default()
                .insert(item.remote_id.clone());
        }
    }

    fn delete_subtree(&mut self, remote_id: &str) {
        let mut queue = VecDeque::from([remote_id.to_owned()]);
        let mut delete_order = Vec::new();

        while let Some(current) = queue.pop_front() {
            if let Some(children) = self.children_by_parent.get(&current) {
                queue.extend(children.iter().cloned());
            }
            delete_order.push(current);
        }

        for current in delete_order.into_iter().rev() {
            self.children_by_parent.remove(&current);

            if let Some(Some(parent_remote_id)) = self.parents.remove(&current) {
                let mut remove_parent_bucket = false;
                if let Some(children) = self.children_by_parent.get_mut(&parent_remote_id) {
                    children.remove(&current);
                    remove_parent_bucket = children.is_empty();
                }
                if remove_parent_bucket {
                    self.children_by_parent.remove(&parent_remote_id);
                }
            }
        }
    }

    fn is_complete(&self) -> bool {
        if self.parents.is_empty() {
            return true;
        }

        let mut queue = VecDeque::from([self.root_remote_id.as_str()]);
        let mut reached = HashSet::with_capacity(self.parents.len());

        while let Some(parent_remote_id) = queue.pop_front() {
            let Some(children) = self.children_by_parent.get(parent_remote_id) else {
                continue;
            };

            for child_remote_id in children {
                if reached.insert(child_remote_id.as_str()) {
                    queue.push_back(child_remote_id.as_str());
                }
            }
        }

        reached.len() == self.parents.len()
    }
}

impl std::fmt::Debug for RootCatalogProjection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RootCatalogProjection")
            .field("item_count", &self.parents.len())
            .finish()
    }
}

fn reject_unexpected_hydration(
    hydration: Option<Vec<RemoteItem>>,
) -> Result<(), RootCatalogProjectionError> {
    if hydration.is_some() {
        Err(RootCatalogProjectionError::UnexpectedHydration)
    } else {
        Ok(())
    }
}

fn validate_hydration(
    expected_root: &RemoteItem,
    hydration: &[RemoteItem],
) -> Result<(), RootCatalogProjectionError> {
    let Some(actual_root) = hydration.first() else {
        return Err(RootCatalogProjectionError::InvalidHydrationRoot);
    };

    if actual_root != expected_root
        || actual_root.kind != RemoteItemKind::Folder
        || actual_root.trashed
    {
        return Err(RootCatalogProjectionError::InvalidHydrationRoot);
    }

    let mut items_by_id = HashMap::with_capacity(hydration.len());

    for item in hydration {
        if item.remote_id.trim().is_empty() || item.name.is_empty() || item.trashed {
            return Err(RootCatalogProjectionError::InvalidHydrationItem);
        }

        if items_by_id.insert(item.remote_id.as_str(), item).is_some() {
            return Err(RootCatalogProjectionError::DuplicateHydrationRemoteId);
        }
    }

    let mut children_by_parent: HashMap<&str, Vec<&str>> = HashMap::new();

    for item in hydration.iter().skip(1) {
        let Some(parent_remote_id) = item.parent_remote_id.as_deref() else {
            return Err(RootCatalogProjectionError::IncompleteHydration);
        };

        if !items_by_id.contains_key(parent_remote_id) {
            return Err(RootCatalogProjectionError::IncompleteHydration);
        }

        children_by_parent
            .entry(parent_remote_id)
            .or_default()
            .push(item.remote_id.as_str());
    }

    let mut queue = VecDeque::from([expected_root.remote_id.as_str()]);
    let mut reached = HashSet::with_capacity(hydration.len());
    reached.insert(expected_root.remote_id.as_str());

    while let Some(parent_remote_id) = queue.pop_front() {
        let Some(children) = children_by_parent.get(parent_remote_id) else {
            continue;
        };

        for child_remote_id in children {
            if reached.insert(*child_remote_id) {
                queue.push_back(*child_remote_id);
            }
        }
    }

    if reached.len() != hydration.len() {
        return Err(RootCatalogProjectionError::IncompleteHydration);
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Divergence {
    pub local_changed_since_checkpoint: bool,
    pub remote_changed_since_checkpoint: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileAction {
    Noop,
    UploadLocal,
    DownloadRemote,
    Conflict,
    IgnoreLocalChange,
    IgnoreRemoteChange,
}

/// Conservative first-stage planner.
///
/// Deletion semantics and rename coalescing are intentionally deferred until
/// their journal invariants are specified. Concurrent changes always become a
/// conflict rather than silently overwriting either side.
pub fn plan(mode: SyncMode, divergence: Divergence) -> ReconcileAction {
    if divergence.local_changed_since_checkpoint && divergence.remote_changed_since_checkpoint {
        return ReconcileAction::Conflict;
    }

    match (
        mode,
        divergence.local_changed_since_checkpoint,
        divergence.remote_changed_since_checkpoint,
    ) {
        (_, false, false) => ReconcileAction::Noop,
        (SyncMode::TwoWay, true, false) | (SyncMode::MirrorLocalToRemote, true, false) => {
            ReconcileAction::UploadLocal
        }
        (SyncMode::TwoWay, false, true) | (SyncMode::ReceiveOnly, false, true) => {
            ReconcileAction::DownloadRemote
        }
        (SyncMode::ReceiveOnly, true, false) => ReconcileAction::IgnoreLocalChange,
        (SyncMode::MirrorLocalToRemote, false, true) => ReconcileAction::IgnoreRemoteChange,
        (_, true, true) => unreachable!("conflict is handled before mode-specific planning"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn remote_item(kind: RemoteItemKind, trashed: bool) -> nubisync_core::RemoteItem {
        nubisync_core::RemoteItem {
            remote_id: "remote-item".into(),
            parent_remote_id: Some("parent".into()),
            name: "item".into(),
            kind,
            size_bytes: Some(10),
            modified_unix_ms: None,
            trashed,
        }
    }

    #[test]
    fn root_change_always_requires_root_revalidation() {
        let root_upsert = RemoteChange::Upsert(remote_item(RemoteItemKind::Folder, false));
        assert_eq!(
            plan_root_catalog_change(&root_upsert, RootChangeMembership::Root, false,),
            Ok(RootCatalogAction::RevalidateRoot)
        );

        let root_delete = RemoteChange::Delete {
            remote_id: "canonical-root".into(),
        };
        assert_eq!(
            plan_root_catalog_change(&root_delete, RootChangeMembership::Root, false,),
            Ok(RootCatalogAction::RevalidateRoot)
        );
    }

    #[test]
    fn descendant_file_upserts_without_hydration() {
        let change = RemoteChange::Upsert(remote_item(RemoteItemKind::File, false));

        for previously_cataloged in [false, true] {
            assert_eq!(
                plan_root_catalog_change(
                    &change,
                    RootChangeMembership::Descendant,
                    previously_cataloged,
                ),
                Ok(RootCatalogAction::UpsertItem)
            );
        }
    }

    #[test]
    fn newly_entering_folder_requires_full_subtree_hydration() {
        let change = RemoteChange::Upsert(remote_item(RemoteItemKind::Folder, false));

        assert_eq!(
            plan_root_catalog_change(&change, RootChangeMembership::Descendant, false,),
            Ok(RootCatalogAction::HydrateSubtree)
        );

        assert_eq!(
            plan_root_catalog_change(&change, RootChangeMembership::Descendant, true,),
            Ok(RootCatalogAction::UpsertItem)
        );
    }

    #[test]
    fn moved_out_cataloged_item_deletes_its_previous_subtree() {
        let folder = RemoteChange::Upsert(remote_item(RemoteItemKind::Folder, false));

        assert_eq!(
            plan_root_catalog_change(&folder, RootChangeMembership::Outside, true,),
            Ok(RootCatalogAction::DeleteSubtree)
        );

        assert_eq!(
            plan_root_catalog_change(&folder, RootChangeMembership::Outside, false,),
            Ok(RootCatalogAction::Ignore)
        );
    }

    #[test]
    fn trashed_descendant_deletes_only_when_previously_cataloged() {
        let trashed = RemoteChange::Upsert(remote_item(RemoteItemKind::Folder, true));

        assert_eq!(
            plan_root_catalog_change(&trashed, RootChangeMembership::Descendant, true,),
            Ok(RootCatalogAction::DeleteSubtree)
        );

        assert_eq!(
            plan_root_catalog_change(&trashed, RootChangeMembership::Descendant, false,),
            Ok(RootCatalogAction::Ignore)
        );
    }

    #[test]
    fn removed_change_uses_catalog_presence_without_guessing_membership() {
        let removed = RemoteChange::Delete {
            remote_id: "removed-item".into(),
        };

        assert_eq!(
            plan_root_catalog_change(&removed, RootChangeMembership::UnresolvedDelete, true,),
            Ok(RootCatalogAction::DeleteSubtree)
        );

        assert_eq!(
            plan_root_catalog_change(&removed, RootChangeMembership::UnresolvedDelete, false,),
            Ok(RootCatalogAction::Ignore)
        );
    }

    #[test]
    fn planner_rejects_impossible_change_contexts() {
        let removed = RemoteChange::Delete {
            remote_id: "removed-item".into(),
        };
        assert_eq!(
            plan_root_catalog_change(&removed, RootChangeMembership::Descendant, true,),
            Err(RootCatalogPlanError::InvalidChangeContext)
        );

        let upsert = RemoteChange::Upsert(remote_item(RemoteItemKind::File, false));
        assert_eq!(
            plan_root_catalog_change(&upsert, RootChangeMembership::UnresolvedDelete, false,),
            Err(RootCatalogPlanError::InvalidChangeContext)
        );
    }

    fn catalog_item(remote_id: &str, parent_remote_id: &str, kind: RemoteItemKind) -> RemoteItem {
        RemoteItem {
            remote_id: remote_id.into(),
            parent_remote_id: Some(parent_remote_id.into()),
            name: format!("{remote_id}.item"),
            kind,
            size_bytes: Some(10),
            modified_unix_ms: None,
            trashed: false,
        }
    }

    #[test]
    fn projection_rejects_incomplete_initial_catalog() {
        let orphan = catalog_item("orphan", "missing-parent", RemoteItemKind::File);

        assert_eq!(
            RootCatalogProjection::new("selected-root", &[orphan]).unwrap_err(),
            RootCatalogProjectionError::IncompleteInitialCatalog
        );
    }

    #[test]
    fn projection_debug_redacts_root_and_remote_metadata() {
        let item = catalog_item("private-file-id", "selected-root", RemoteItemKind::File);
        let projection = RootCatalogProjection::new("private-root-id", &[]).unwrap();
        let debug = format!("{projection:?}");

        assert_eq!(debug, "RootCatalogProjection { item_count: 0 }");
        assert!(!debug.contains("private-root-id"));
        assert!(!debug.contains(&item.remote_id));
        assert!(!debug.contains(&item.name));
    }

    #[test]
    fn hydration_is_required_exact_and_connected() {
        let entering_folder = catalog_item("folder-in", "selected-root", RemoteItemKind::Folder);
        let child = catalog_item("child", "folder-in", RemoteItemKind::File);
        let change = RemoteChange::Upsert(entering_folder.clone());
        let mut projection = RootCatalogProjection::new("selected-root", &[]).unwrap();

        assert_eq!(
            projection
                .apply_change(&change, RootChangeMembership::Descendant, None,)
                .unwrap_err(),
            RootCatalogProjectionError::HydrationRequired
        );

        let wrong_root = catalog_item("different-folder", "selected-root", RemoteItemKind::Folder);
        assert_eq!(
            projection
                .apply_change(
                    &change,
                    RootChangeMembership::Descendant,
                    Some(vec![wrong_root]),
                )
                .unwrap_err(),
            RootCatalogProjectionError::InvalidHydrationRoot
        );

        let disconnected = catalog_item("orphan", "missing", RemoteItemKind::File);
        assert_eq!(
            projection
                .apply_change(
                    &change,
                    RootChangeMembership::Descendant,
                    Some(vec![entering_folder.clone(), disconnected]),
                )
                .unwrap_err(),
            RootCatalogProjectionError::IncompleteHydration
        );

        let resolution = projection
            .apply_change(
                &change,
                RootChangeMembership::Descendant,
                Some(vec![entering_folder.clone(), child.clone()]),
            )
            .unwrap();

        assert_eq!(
            resolution,
            RootCatalogResolution::Mutations(vec![
                RootCatalogMutationPlan::Upsert(entering_folder),
                RootCatalogMutationPlan::Upsert(child),
            ])
        );
        assert_eq!(projection.item_count(), 2);
        projection.validate_complete().unwrap();
    }

    #[test]
    fn projection_tracks_items_introduced_earlier_in_same_batch() {
        let entering_folder = catalog_item("folder-in", "selected-root", RemoteItemKind::Folder);
        let child = catalog_item("child", "folder-in", RemoteItemKind::File);
        let folder_change = RemoteChange::Upsert(entering_folder.clone());

        let mut projection = RootCatalogProjection::new("selected-root", &[]).unwrap();

        projection
            .apply_change(
                &folder_change,
                RootChangeMembership::Descendant,
                Some(vec![entering_folder, child.clone()]),
            )
            .unwrap();

        assert!(projection.contains("child"));

        let delete_child = RemoteChange::Delete {
            remote_id: child.remote_id.clone(),
        };

        let resolution = projection
            .apply_change(&delete_child, RootChangeMembership::UnresolvedDelete, None)
            .unwrap();

        assert_eq!(
            resolution,
            RootCatalogResolution::Mutations(vec![RootCatalogMutationPlan::DeleteSubtree {
                remote_id: child.remote_id,
            },])
        );
        assert!(!projection.contains("child"));
        assert!(projection.contains("folder-in"));
        projection.validate_complete().unwrap();
    }

    #[test]
    fn projection_delete_subtree_removes_descendants_for_later_changes() {
        let folder = catalog_item("folder", "selected-root", RemoteItemKind::Folder);
        let nested_folder = catalog_item("nested", "folder", RemoteItemKind::Folder);
        let nested_file = catalog_item("file", "nested", RemoteItemKind::File);

        let mut projection = RootCatalogProjection::new(
            "selected-root",
            &[folder.clone(), nested_folder, nested_file],
        )
        .unwrap();

        let moved_out = RemoteChange::Upsert(RemoteItem {
            parent_remote_id: Some("outside".into()),
            ..folder
        });

        let resolution = projection
            .apply_change(&moved_out, RootChangeMembership::Outside, None)
            .unwrap();

        assert_eq!(
            resolution,
            RootCatalogResolution::Mutations(vec![RootCatalogMutationPlan::DeleteSubtree {
                remote_id: "folder".into(),
            },])
        );
        assert_eq!(projection.item_count(), 0);
        projection.validate_complete().unwrap();

        let removed_nested = RemoteChange::Delete {
            remote_id: "file".into(),
        };
        assert_eq!(
            projection
                .apply_change(
                    &removed_nested,
                    RootChangeMembership::UnresolvedDelete,
                    None,
                )
                .unwrap(),
            RootCatalogResolution::Noop
        );
    }

    #[test]
    fn projection_detects_unresolved_parent_gap_before_commit() {
        let mut projection = RootCatalogProjection::new("selected-root", &[]).unwrap();

        let file = catalog_item("new-file", "not-yet-known-parent", RemoteItemKind::File);
        let change = RemoteChange::Upsert(file);

        projection
            .apply_change(&change, RootChangeMembership::Descendant, None)
            .unwrap();

        assert_eq!(
            projection.validate_complete().unwrap_err(),
            RootCatalogProjectionError::IncompleteProjectedCatalog
        );
    }

    #[test]
    fn projection_allows_parent_arriving_later_in_same_batch() {
        let mut projection = RootCatalogProjection::new("selected-root", &[]).unwrap();

        let child = catalog_item("child", "parent", RemoteItemKind::File);
        projection
            .apply_change(
                &RemoteChange::Upsert(child),
                RootChangeMembership::Descendant,
                None,
            )
            .unwrap();

        assert_eq!(
            projection.validate_complete().unwrap_err(),
            RootCatalogProjectionError::IncompleteProjectedCatalog
        );

        let parent = catalog_item("parent", "selected-root", RemoteItemKind::Folder);
        projection
            .apply_change(
                &RemoteChange::Upsert(parent.clone()),
                RootChangeMembership::Descendant,
                Some(vec![parent]),
            )
            .unwrap();

        projection.validate_complete().unwrap();
        assert_eq!(projection.item_count(), 2);
    }

    #[test]
    fn projection_requires_root_revalidation_without_mutating_catalog() {
        let file = catalog_item("file", "selected-root", RemoteItemKind::File);
        let mut projection =
            RootCatalogProjection::new("selected-root", std::slice::from_ref(&file)).unwrap();

        let root_change = RemoteChange::Delete {
            remote_id: "selected-root".into(),
        };

        assert_eq!(
            projection
                .apply_change(&root_change, RootChangeMembership::Root, None,)
                .unwrap(),
            RootCatalogResolution::RevalidateRoot
        );
        assert_eq!(projection.item_count(), 1);
        assert!(projection.contains(&file.remote_id));
    }

    #[test]
    fn simultaneous_changes_fail_safe_as_conflict() {
        for mode in [
            SyncMode::TwoWay,
            SyncMode::MirrorLocalToRemote,
            SyncMode::ReceiveOnly,
        ] {
            assert_eq!(
                plan(
                    mode,
                    Divergence {
                        local_changed_since_checkpoint: true,
                        remote_changed_since_checkpoint: true,
                    }
                ),
                ReconcileAction::Conflict
            );
        }
    }

    #[test]
    fn receive_only_never_uploads_local_change() {
        assert_eq!(
            plan(
                SyncMode::ReceiveOnly,
                Divergence {
                    local_changed_since_checkpoint: true,
                    remote_changed_since_checkpoint: false,
                }
            ),
            ReconcileAction::IgnoreLocalChange
        );
    }
}
