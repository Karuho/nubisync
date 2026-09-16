//! Synchronization planning primitives.

#![forbid(unsafe_code)]

mod convergence;

pub use convergence::{
    ReceiveOnlyConvergenceAction, ReceiveOnlyConvergenceActionKind, ReceiveOnlyConvergencePlan,
    ReceiveOnlyConvergencePlanError, ReceiveOnlyOwnershipReceipt, ReceiveOnlyReceiptState,
    plan_receive_only_convergence,
};
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
pub enum LocalTreeEntryKind {
    File,
    Directory,
}

#[derive(Clone, PartialEq, Eq)]
pub struct LocalTreeEntry {
    relative_path: String,
    pub kind: LocalTreeEntryKind,
}

impl LocalTreeEntry {
    pub fn new(
        relative_path: impl Into<String>,
        kind: LocalTreeEntryKind,
    ) -> Result<Self, ReceiveOnlyMaterializationPlanError> {
        let relative_path = relative_path.into();
        validate_safe_relative_path(&relative_path)?;
        Ok(Self {
            relative_path,
            kind,
        })
    }

    fn relative_path(&self) -> &str {
        &self.relative_path
    }
}

impl std::fmt::Debug for LocalTreeEntry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalTreeEntry")
            .field("kind", &self.kind)
            .field("relative_path", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveOnlyMaterializationPlan {
    pub remote_items: usize,
    pub remote_directories: usize,
    pub remote_files: usize,
    pub local_entries: usize,
    pub missing_directories: usize,
    pub missing_files: usize,
    pub matching_directories: usize,
    pub existing_files_unverified: usize,
    pub local_only_entries: usize,
    pub type_conflicts: usize,
}

impl ReceiveOnlyMaterializationPlan {
    pub fn ready_for_directory_phase(&self) -> bool {
        self.type_conflicts == 0 && self.local_only_entries == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiveOnlyMaterializationPlanError {
    InvalidRemoteItem,
    UnsafeRemoteName,
    DuplicateRemoteId,
    DuplicateSiblingName,
    IncompleteRemoteCatalog,
    InvalidRemoteParentKind,
    RemotePathCollision,
    InvalidLocalRelativePath,
    DuplicateLocalPath,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ReceiveOnlyDirectoryTarget {
    remote_id: String,
    relative_path: String,
}

impl ReceiveOnlyDirectoryTarget {
    pub fn remote_id(&self) -> &str {
        &self.remote_id
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }
}

impl std::fmt::Debug for ReceiveOnlyDirectoryTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReceiveOnlyDirectoryTarget")
            .field("remote_id", &"[redacted]")
            .field("relative_path", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ReceiveOnlyFileTarget {
    remote_id: String,
    relative_path: String,
    size_bytes: Option<u64>,
}

impl ReceiveOnlyFileTarget {
    pub fn remote_id(&self) -> &str {
        &self.remote_id
    }
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }
    pub fn size_bytes(&self) -> Option<u64> {
        self.size_bytes
    }
}

impl std::fmt::Debug for ReceiveOnlyFileTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReceiveOnlyFileTarget")
            .field("remote_id", &"[redacted]")
            .field("relative_path", &"[redacted]")
            .field("size_bytes", &self.size_bytes)
            .finish()
    }
}

pub fn plan_receive_only_materialization(
    remote_items: &[RemoteItem],
    local_entries: &[LocalTreeEntry],
) -> Result<ReceiveOnlyMaterializationPlan, ReceiveOnlyMaterializationPlanError> {
    let expected_paths = build_remote_expected_paths(remote_items)?;
    let mut local_by_path = HashMap::with_capacity(local_entries.len());

    for entry in local_entries {
        validate_safe_relative_path(entry.relative_path())?;

        if local_by_path
            .insert(entry.relative_path().to_owned(), entry.kind)
            .is_some()
        {
            return Err(ReceiveOnlyMaterializationPlanError::DuplicateLocalPath);
        }
    }

    let remote_directories = expected_paths
        .iter()
        .filter(|(_, kind, _, _)| *kind == RemoteItemKind::Folder)
        .count();
    let remote_files = expected_paths
        .iter()
        .filter(|(_, kind, _, _)| *kind == RemoteItemKind::File)
        .count();

    let mut missing_directories = 0_usize;
    let mut missing_files = 0_usize;
    let mut matching_directories = 0_usize;
    let mut existing_files_unverified = 0_usize;
    let mut type_conflicts = 0_usize;

    for (relative_path, remote_kind, _, _) in &expected_paths {
        match (remote_kind, local_by_path.get(relative_path.as_str())) {
            (RemoteItemKind::Folder, None) => missing_directories += 1,
            (RemoteItemKind::File, None) => missing_files += 1,
            (RemoteItemKind::Folder, Some(LocalTreeEntryKind::Directory)) => {
                matching_directories += 1
            }
            (RemoteItemKind::File, Some(LocalTreeEntryKind::File)) => {
                existing_files_unverified += 1
            }
            (RemoteItemKind::Folder, Some(LocalTreeEntryKind::File))
            | (RemoteItemKind::File, Some(LocalTreeEntryKind::Directory)) => type_conflicts += 1,
        }
    }

    let expected_path_names = expected_paths
        .iter()
        .map(|(path, _, _, _)| path.as_str())
        .collect::<HashSet<_>>();

    let local_only_entries = local_by_path
        .keys()
        .filter(|path| !expected_path_names.contains(path.as_str()))
        .count();

    Ok(ReceiveOnlyMaterializationPlan {
        remote_items: remote_items.len(),
        remote_directories,
        remote_files,
        local_entries: local_entries.len(),
        missing_directories,
        missing_files,
        matching_directories,
        existing_files_unverified,
        local_only_entries,
        type_conflicts,
    })
}

pub fn plan_receive_only_directory_targets(
    remote_items: &[RemoteItem],
) -> Result<Vec<ReceiveOnlyDirectoryTarget>, ReceiveOnlyMaterializationPlanError> {
    Ok(build_remote_expected_paths(remote_items)?
        .into_iter()
        .filter_map(|(relative_path, kind, remote_id, _)| {
            (kind == RemoteItemKind::Folder).then_some(ReceiveOnlyDirectoryTarget {
                remote_id,
                relative_path,
            })
        })
        .collect())
}

pub fn plan_receive_only_missing_directory_targets(
    remote_items: &[RemoteItem],
    local_entries: &[LocalTreeEntry],
) -> Result<Vec<ReceiveOnlyDirectoryTarget>, ReceiveOnlyMaterializationPlanError> {
    let _ = plan_receive_only_materialization(remote_items, local_entries)?;
    let local_paths = local_entries
        .iter()
        .map(LocalTreeEntry::relative_path)
        .collect::<HashSet<_>>();

    Ok(build_remote_expected_paths(remote_items)?
        .into_iter()
        .filter_map(|(relative_path, kind, remote_id, _)| {
            (kind == RemoteItemKind::Folder && !local_paths.contains(relative_path.as_str()))
                .then_some(ReceiveOnlyDirectoryTarget {
                    remote_id,
                    relative_path,
                })
        })
        .collect())
}

pub fn plan_receive_only_missing_file_targets(
    remote_items: &[RemoteItem],
    local_entries: &[LocalTreeEntry],
) -> Result<Vec<ReceiveOnlyFileTarget>, ReceiveOnlyMaterializationPlanError> {
    let _ = plan_receive_only_materialization(remote_items, local_entries)?;
    let local_paths = local_entries
        .iter()
        .map(|e| e.relative_path())
        .collect::<HashSet<_>>();

    Ok(build_remote_expected_paths(remote_items)?
        .into_iter()
        .filter_map(|(relative_path, kind, remote_id, size_bytes)| {
            (kind == RemoteItemKind::File && !local_paths.contains(relative_path.as_str()))
                .then_some(ReceiveOnlyFileTarget {
                    remote_id,
                    relative_path,
                    size_bytes,
                })
        })
        .collect())
}

type RemoteExpectedPath = (String, RemoteItemKind, String, Option<u64>);

pub fn plan_receive_only_existing_file_targets(
    remote_items: &[RemoteItem],
    local_entries: &[LocalTreeEntry],
) -> Result<Vec<ReceiveOnlyFileTarget>, ReceiveOnlyMaterializationPlanError> {
    let _ = plan_receive_only_materialization(remote_items, local_entries)?;
    let local_files = local_entries
        .iter()
        .filter(|entry| entry.kind == LocalTreeEntryKind::File)
        .map(LocalTreeEntry::relative_path)
        .collect::<HashSet<_>>();

    Ok(build_remote_expected_paths(remote_items)?
        .into_iter()
        .filter_map(|(relative_path, kind, remote_id, size_bytes)| {
            (kind == RemoteItemKind::File && local_files.contains(relative_path.as_str()))
                .then_some(ReceiveOnlyFileTarget {
                    remote_id,
                    relative_path,
                    size_bytes,
                })
        })
        .collect())
}

fn build_remote_expected_paths(
    remote_items: &[RemoteItem],
) -> Result<Vec<RemoteExpectedPath>, ReceiveOnlyMaterializationPlanError> {
    if remote_items.is_empty() {
        return Ok(Vec::new());
    }

    let mut remote_ids = HashSet::with_capacity(remote_items.len());
    let mut kind_by_id = HashMap::with_capacity(remote_items.len());

    for item in remote_items {
        if item.remote_id.trim().is_empty() || item.parent_remote_id.is_none() || item.trashed {
            return Err(ReceiveOnlyMaterializationPlanError::InvalidRemoteItem);
        }

        validate_safe_remote_name(&item.name)?;

        if !remote_ids.insert(item.remote_id.as_str()) {
            return Err(ReceiveOnlyMaterializationPlanError::DuplicateRemoteId);
        }

        kind_by_id.insert(item.remote_id.as_str(), item.kind);
    }

    let mut external_parent_ids = HashSet::new();
    let mut children_by_parent: HashMap<&str, Vec<&RemoteItem>> = HashMap::new();
    let mut sibling_names = HashSet::with_capacity(remote_items.len());

    for item in remote_items {
        let parent_remote_id = item
            .parent_remote_id
            .as_deref()
            .ok_or(ReceiveOnlyMaterializationPlanError::InvalidRemoteItem)?;

        if !sibling_names.insert((parent_remote_id, item.name.as_str())) {
            return Err(ReceiveOnlyMaterializationPlanError::DuplicateSiblingName);
        }

        match kind_by_id.get(parent_remote_id) {
            Some(RemoteItemKind::Folder) => {}
            Some(RemoteItemKind::File) => {
                return Err(ReceiveOnlyMaterializationPlanError::InvalidRemoteParentKind);
            }
            None => {
                external_parent_ids.insert(parent_remote_id);
            }
        }

        children_by_parent
            .entry(parent_remote_id)
            .or_default()
            .push(item);
    }

    if external_parent_ids.len() != 1 {
        return Err(ReceiveOnlyMaterializationPlanError::IncompleteRemoteCatalog);
    }

    let root_remote_id = *external_parent_ids
        .iter()
        .next()
        .ok_or(ReceiveOnlyMaterializationPlanError::IncompleteRemoteCatalog)?;

    let mut queue: VecDeque<(&RemoteItem, String)> = VecDeque::new();

    if let Some(top_level) = children_by_parent.get(root_remote_id) {
        let mut top_level = top_level.clone();
        top_level.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.remote_id.cmp(&right.remote_id))
        });

        for item in top_level {
            queue.push_back((item, item.name.clone()));
        }
    }

    let mut visited = HashSet::with_capacity(remote_items.len());
    let mut seen_paths = HashSet::with_capacity(remote_items.len());
    let mut expected_paths = Vec::with_capacity(remote_items.len());

    while let Some((item, relative_path)) = queue.pop_front() {
        validate_safe_relative_path(&relative_path)?;

        if !visited.insert(item.remote_id.as_str()) {
            return Err(ReceiveOnlyMaterializationPlanError::IncompleteRemoteCatalog);
        }

        if !seen_paths.insert(relative_path.clone()) {
            return Err(ReceiveOnlyMaterializationPlanError::RemotePathCollision);
        }

        expected_paths.push((
            relative_path.clone(),
            item.kind,
            item.remote_id.clone(),
            item.size_bytes,
        ));

        if item.kind == RemoteItemKind::Folder
            && let Some(children) = children_by_parent.get(item.remote_id.as_str())
        {
            let mut children = children.clone();
            children.sort_by(|left, right| {
                left.name
                    .cmp(&right.name)
                    .then_with(|| left.remote_id.cmp(&right.remote_id))
            });

            for child in children {
                let child_path = format!("{relative_path}/{}", child.name);
                validate_safe_relative_path(&child_path)?;
                queue.push_back((child, child_path));
            }
        }
    }

    if visited.len() != remote_items.len() {
        return Err(ReceiveOnlyMaterializationPlanError::IncompleteRemoteCatalog);
    }

    Ok(expected_paths)
}

fn validate_safe_remote_name(name: &str) -> Result<(), ReceiveOnlyMaterializationPlanError> {
    if name.is_empty() || matches!(name, "." | "..") || name.contains('/') || name.contains('\0') {
        return Err(ReceiveOnlyMaterializationPlanError::UnsafeRemoteName);
    }

    Ok(())
}

fn validate_safe_relative_path(
    relative_path: &str,
) -> Result<(), ReceiveOnlyMaterializationPlanError> {
    if relative_path.is_empty()
        || relative_path.starts_with('/')
        || relative_path.ends_with('/')
        || relative_path.contains('\0')
    {
        return Err(ReceiveOnlyMaterializationPlanError::InvalidLocalRelativePath);
    }

    for component in relative_path.split('/') {
        if component.is_empty() || matches!(component, "." | "..") {
            return Err(ReceiveOnlyMaterializationPlanError::InvalidLocalRelativePath);
        }
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

    fn materialization_item(
        remote_id: &str,
        parent_remote_id: &str,
        name: &str,
        kind: RemoteItemKind,
    ) -> RemoteItem {
        RemoteItem {
            remote_id: remote_id.into(),
            parent_remote_id: Some(parent_remote_id.into()),
            name: name.into(),
            kind,
            size_bytes: None,
            modified_unix_ms: None,
            trashed: false,
        }
    }

    #[test]
    fn materialization_plan_counts_missing_remote_tree() {
        let folder =
            materialization_item("folder", "selected-root", "folder", RemoteItemKind::Folder);
        let file = materialization_item("file", "folder", "file.txt", RemoteItemKind::File);

        let plan = plan_receive_only_materialization(&[folder, file], &[]).unwrap();

        assert_eq!(plan.remote_items, 2);
        assert_eq!(plan.remote_directories, 1);
        assert_eq!(plan.remote_files, 1);
        assert_eq!(plan.local_entries, 0);
        assert_eq!(plan.missing_directories, 1);
        assert_eq!(plan.missing_files, 1);
        assert_eq!(plan.type_conflicts, 0);
        assert!(plan.ready_for_directory_phase());
    }

    #[test]
    fn materialization_plan_rejects_unsafe_names_and_duplicate_siblings() {
        let unsafe_item = materialization_item(
            "unsafe",
            "selected-root",
            "../escape",
            RemoteItemKind::Folder,
        );

        assert_eq!(
            plan_receive_only_materialization(&[unsafe_item], &[]).unwrap_err(),
            ReceiveOnlyMaterializationPlanError::UnsafeRemoteName
        );

        let first = materialization_item("one", "selected-root", "same", RemoteItemKind::Folder);
        let second = materialization_item("two", "selected-root", "same", RemoteItemKind::File);

        assert_eq!(
            plan_receive_only_materialization(&[first, second], &[]).unwrap_err(),
            ReceiveOnlyMaterializationPlanError::DuplicateSiblingName
        );
    }

    #[test]
    fn materialization_plan_detects_local_conflicts_without_mutating() {
        let folder =
            materialization_item("folder", "selected-root", "docs", RemoteItemKind::Folder);
        let file =
            materialization_item("file", "selected-root", "readme.txt", RemoteItemKind::File);

        let local = vec![
            LocalTreeEntry::new("docs", LocalTreeEntryKind::File).unwrap(),
            LocalTreeEntry::new("readme.txt", LocalTreeEntryKind::File).unwrap(),
            LocalTreeEntry::new("local-only", LocalTreeEntryKind::Directory).unwrap(),
        ];

        let plan = plan_receive_only_materialization(&[folder, file], &local).unwrap();

        assert_eq!(plan.type_conflicts, 1);
        assert_eq!(plan.existing_files_unverified, 1);
        assert_eq!(plan.local_only_entries, 1);
        assert!(!plan.ready_for_directory_phase());
    }

    #[test]
    fn local_tree_entry_debug_redacts_relative_path() {
        let entry = LocalTreeEntry::new("private/folder", LocalTreeEntryKind::Directory).unwrap();
        let debug = format!("{entry:?}");

        assert!(debug.contains("[redacted]"));
        assert!(!debug.contains("private"));
        assert!(!debug.contains("folder"));
    }

    #[test]
    fn directory_targets_are_parent_first_and_debug_redacted() {
        let folder =
            materialization_item("folder", "selected-root", "docs", RemoteItemKind::Folder);
        let nested = materialization_item("nested", "folder", "nested", RemoteItemKind::Folder);
        let file = materialization_item("file", "nested", "readme.txt", RemoteItemKind::File);

        let targets = plan_receive_only_directory_targets(&[folder, nested, file]).unwrap();

        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].relative_path(), "docs");
        assert_eq!(targets[1].relative_path(), "docs/nested");

        let debug = format!("{targets:?}");
        assert!(debug.contains("[redacted]"));
        assert!(!debug.contains("docs"));
        assert!(!debug.contains("nested"));
    }

    #[test]
    fn missing_file_target_preserves_size_and_redacts_identity() {
        let folder =
            materialization_item("folder", "selected-root", "docs", RemoteItemKind::Folder);
        let mut file =
            materialization_item("secret-id", "folder", "private.txt", RemoteItemKind::File);
        file.size_bytes = Some(13);
        let local = vec![LocalTreeEntry::new("docs", LocalTreeEntryKind::Directory).unwrap()];
        let targets = plan_receive_only_missing_file_targets(&[folder, file], &local).unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].size_bytes(), Some(13));
        let debug = format!("{targets:?}");
        assert!(!debug.contains("private.txt"));
        assert!(!debug.contains("secret-id"));
    }

    #[test]
    fn materialization_rejects_children_of_remote_files() {
        let file = materialization_item(
            "file-parent",
            "selected-root",
            "file.bin",
            RemoteItemKind::File,
        );
        let child = materialization_item("child", "file-parent", "child", RemoteItemKind::Folder);

        assert_eq!(
            plan_receive_only_materialization(&[file, child], &[]).unwrap_err(),
            ReceiveOnlyMaterializationPlanError::InvalidRemoteParentKind
        );
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

#[cfg(test)]
mod phase5d2_tests {
    use super::*;

    fn directory_item(remote_id: &str, parent_remote_id: &str, name: &str) -> RemoteItem {
        RemoteItem {
            remote_id: remote_id.into(),
            parent_remote_id: Some(parent_remote_id.into()),
            name: name.into(),
            kind: RemoteItemKind::Folder,
            size_bytes: None,
            modified_unix_ms: None,
            trashed: false,
        }
    }

    #[test]
    fn missing_directory_targets_are_parent_before_child_and_exclude_existing() {
        let remote_items = vec![
            directory_item("parent", "selected", "parent"),
            directory_item("child", "parent", "child"),
        ];

        let empty_targets =
            plan_receive_only_missing_directory_targets(&remote_items, &[]).unwrap();
        assert_eq!(empty_targets.len(), 2);
        assert_eq!(empty_targets[0].remote_id(), "parent");
        assert_eq!(empty_targets[1].remote_id(), "child");

        let local_entries =
            vec![LocalTreeEntry::new("parent", LocalTreeEntryKind::Directory).unwrap()];
        let partial_targets =
            plan_receive_only_missing_directory_targets(&remote_items, &local_entries).unwrap();

        assert_eq!(partial_targets.len(), 1);
        assert_eq!(partial_targets[0].remote_id(), "child");
    }
}
