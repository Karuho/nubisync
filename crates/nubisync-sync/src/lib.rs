//! Synchronization planning primitives.

#![forbid(unsafe_code)]

pub use nubisync_core::SyncMode;
use nubisync_core::{RemoteChange, RemoteItemKind};

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
