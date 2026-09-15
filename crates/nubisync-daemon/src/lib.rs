//! NubiSync synchronization orchestration owned by the daemon layer.

#![forbid(unsafe_code)]

use nubisync_core::{ChangeCursor, RemoteChange, RemoteItem, RemoteItemKind, SyncRoot};
use nubisync_drive::{DriveApiError, DriveFolderRoot, DriveRootMembership, GoogleDriveApi};
use nubisync_storage::{
    Storage, StorageError, SyncRootCatalogBatchCommit, SyncRootCatalogMutation,
};
use nubisync_sync::{
    RootCatalogMutationPlan, RootCatalogProjection, RootCatalogProjectionError,
    RootCatalogResolution, RootChangeMembership,
};
use thiserror::Error;

pub trait SelectedRootProvider {
    type RootIdentity;

    fn resolve_root(
        &self,
        remote_root_id: &str,
    ) -> Result<Self::RootIdentity, SelectedRootExecutorError>;

    fn canonical_root_id<'a>(&self, root: &'a Self::RootIdentity) -> &'a str;

    fn resolve_membership(
        &self,
        item: &RemoteItem,
        root: &Self::RootIdentity,
    ) -> Result<RootChangeMembership, SelectedRootExecutorError>;

    fn hydrate_folder(
        &self,
        item: &RemoteItem,
    ) -> Result<Vec<RemoteItem>, SelectedRootExecutorError>;
}

impl SelectedRootProvider for GoogleDriveApi {
    type RootIdentity = DriveFolderRoot;

    fn resolve_root(
        &self,
        remote_root_id: &str,
    ) -> Result<Self::RootIdentity, SelectedRootExecutorError> {
        GoogleDriveApi::resolve_folder_root(self, remote_root_id)
            .map_err(SelectedRootExecutorError::from)
    }

    fn canonical_root_id<'a>(&self, root: &'a Self::RootIdentity) -> &'a str {
        root.canonical_remote_id()
    }

    fn resolve_membership(
        &self,
        item: &RemoteItem,
        root: &Self::RootIdentity,
    ) -> Result<RootChangeMembership, SelectedRootExecutorError> {
        let membership = GoogleDriveApi::resolve_item_membership(self, item, root)
            .map_err(SelectedRootExecutorError::from)?;

        Ok(match membership {
            DriveRootMembership::Root => RootChangeMembership::Root,
            DriveRootMembership::Descendant => RootChangeMembership::Descendant,
            DriveRootMembership::Outside => RootChangeMembership::Outside,
        })
    }

    fn hydrate_folder(
        &self,
        item: &RemoteItem,
    ) -> Result<Vec<RemoteItem>, SelectedRootExecutorError> {
        GoogleDriveApi::hydrate_folder_subtree(self, item)
            .map(|hydration| hydration.into_items())
            .map_err(SelectedRootExecutorError::from)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootBatchExecution {
    pub provider_changes: usize,
    pub storage_mutations: usize,
    pub authoritative_items: u64,
    pub hydrated_items: usize,
    pub root_revalidations: usize,
    pub completed_initial_catchup: bool,
}

pub fn execute_selected_root_change_batch<P: SelectedRootProvider>(
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
    expected_cursor: &ChangeCursor,
    changes: &[RemoteChange],
    next_cursor: &ChangeCursor,
    observed_at_unix_ms: i64,
) -> Result<SelectedRootBatchExecution, SelectedRootExecutorError> {
    let configured_remote_root_id = sync_root
        .remote_root_id
        .as_deref()
        .ok_or(SelectedRootExecutorError::MissingRemoteRoot)?;

    let root_identity = provider.resolve_root(configured_remote_root_id)?;
    let canonical_root_id = provider.canonical_root_id(&root_identity).to_owned();

    if canonical_root_id.trim().is_empty() {
        return Err(SelectedRootExecutorError::InvalidCanonicalRoot);
    }

    let durable_items = storage.list_sync_root_remote_items(&sync_root.id)?;
    let mut projection = RootCatalogProjection::new(&canonical_root_id, &durable_items)?;

    let mut mutation_plans = Vec::new();
    let mut hydrated_items = 0_usize;
    let mut root_revalidations = 0_usize;

    for change in changes {
        let membership =
            resolve_change_membership(provider, &root_identity, &canonical_root_id, change)?;

        let hydration = match change {
            RemoteChange::Upsert(item)
                if membership == RootChangeMembership::Descendant
                    && item.kind == RemoteItemKind::Folder
                    && !item.trashed
                    && !projection.contains(&item.remote_id) =>
            {
                let items = provider.hydrate_folder(item)?;
                hydrated_items = hydrated_items
                    .checked_add(items.len())
                    .ok_or(SelectedRootExecutorError::CountOverflow)?;
                Some(items)
            }
            _ => None,
        };

        match projection.apply_change(change, membership, hydration)? {
            RootCatalogResolution::Noop => {}
            RootCatalogResolution::Mutations(mut planned) => {
                mutation_plans.append(&mut planned);
            }
            RootCatalogResolution::RevalidateRoot => {
                root_revalidations = root_revalidations
                    .checked_add(1)
                    .ok_or(SelectedRootExecutorError::CountOverflow)?;

                let revalidated = provider.resolve_root(configured_remote_root_id)?;
                if provider.canonical_root_id(&revalidated) != canonical_root_id {
                    return Err(SelectedRootExecutorError::RootIdentityChanged);
                }
            }
        }
    }

    projection.validate_complete()?;

    let storage_mutations = mutation_plans
        .into_iter()
        .map(into_storage_mutation)
        .collect::<Vec<_>>();

    let commit = storage.commit_sync_root_catalog_batch_and_cursor(
        &sync_root.id,
        expected_cursor,
        &storage_mutations,
        next_cursor,
        observed_at_unix_ms,
    )?;

    Ok(execution_result(
        changes.len(),
        storage_mutations.len(),
        hydrated_items,
        root_revalidations,
        commit,
    ))
}

fn resolve_change_membership<P: SelectedRootProvider>(
    provider: &P,
    root_identity: &P::RootIdentity,
    canonical_root_id: &str,
    change: &RemoteChange,
) -> Result<RootChangeMembership, SelectedRootExecutorError> {
    match change {
        RemoteChange::Delete { remote_id } if remote_id == canonical_root_id => {
            Ok(RootChangeMembership::Root)
        }
        RemoteChange::Delete { .. } => Ok(RootChangeMembership::UnresolvedDelete),
        RemoteChange::Upsert(item) => provider.resolve_membership(item, root_identity),
    }
}

fn into_storage_mutation(plan: RootCatalogMutationPlan) -> SyncRootCatalogMutation {
    match plan {
        RootCatalogMutationPlan::Upsert(item) => SyncRootCatalogMutation::Upsert(item),
        RootCatalogMutationPlan::DeleteSubtree { remote_id } => {
            SyncRootCatalogMutation::DeleteSubtree { remote_id }
        }
    }
}

fn execution_result(
    provider_changes: usize,
    storage_mutations: usize,
    hydrated_items: usize,
    root_revalidations: usize,
    commit: SyncRootCatalogBatchCommit,
) -> SelectedRootBatchExecution {
    SelectedRootBatchExecution {
        provider_changes,
        storage_mutations,
        authoritative_items: commit.authoritative_items,
        hydrated_items,
        root_revalidations,
        completed_initial_catchup: commit.completed_initial_catchup,
    }
}

#[derive(Debug, Error)]
pub enum SelectedRootExecutorError {
    #[error("selected sync root has no configured remote root")]
    MissingRemoteRoot,
    #[error("provider returned an invalid canonical root identity")]
    InvalidCanonicalRoot,
    #[error("selected root canonical identity changed during batch execution")]
    RootIdentityChanged,
    #[error("selected-root batch counter overflowed")]
    CountOverflow,
    #[error("selected-root provider operation failed")]
    ProviderOperationFailed,
    #[error("Drive provider operation failed")]
    Drive(Box<DriveApiError>),
    #[error("selected-root storage operation failed")]
    Storage(Box<StorageError>),
    #[error("selected-root catalog projection failed: {0:?}")]
    Projection(RootCatalogProjectionError),
}

impl From<DriveApiError> for SelectedRootExecutorError {
    fn from(error: DriveApiError) -> Self {
        Self::Drive(Box::new(error))
    }
}

impl From<StorageError> for SelectedRootExecutorError {
    fn from(error: StorageError) -> Self {
        Self::Storage(Box::new(error))
    }
}

impl From<RootCatalogProjectionError> for SelectedRootExecutorError {
    fn from(error: RootCatalogProjectionError) -> Self {
        Self::Projection(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nubisync_core::{ProviderAccount, ProviderId, SyncMode};
    use std::{cell::Cell, collections::HashMap};

    struct FakeProvider {
        canonical_root_id: String,
        hydrations: HashMap<String, Vec<RemoteItem>>,
        fail_revalidation: bool,
        root_resolutions: Cell<usize>,
    }

    impl FakeProvider {
        fn new(canonical_root_id: &str) -> Self {
            Self {
                canonical_root_id: canonical_root_id.into(),
                hydrations: HashMap::new(),
                fail_revalidation: false,
                root_resolutions: Cell::new(0),
            }
        }
    }

    impl SelectedRootProvider for FakeProvider {
        type RootIdentity = String;

        fn resolve_root(
            &self,
            _remote_root_id: &str,
        ) -> Result<Self::RootIdentity, SelectedRootExecutorError> {
            let call = self.root_resolutions.get() + 1;
            self.root_resolutions.set(call);

            if self.fail_revalidation && call > 1 {
                return Err(SelectedRootExecutorError::ProviderOperationFailed);
            }

            Ok(self.canonical_root_id.clone())
        }

        fn canonical_root_id<'a>(&self, root: &'a Self::RootIdentity) -> &'a str {
            root
        }

        fn resolve_membership(
            &self,
            item: &RemoteItem,
            root: &Self::RootIdentity,
        ) -> Result<RootChangeMembership, SelectedRootExecutorError> {
            if item.remote_id == *root {
                Ok(RootChangeMembership::Root)
            } else if item.parent_remote_id.as_deref() == Some("outside") {
                Ok(RootChangeMembership::Outside)
            } else {
                Ok(RootChangeMembership::Descendant)
            }
        }

        fn hydrate_folder(
            &self,
            item: &RemoteItem,
        ) -> Result<Vec<RemoteItem>, SelectedRootExecutorError> {
            self.hydrations
                .get(&item.remote_id)
                .cloned()
                .ok_or(SelectedRootExecutorError::ProviderOperationFailed)
        }
    }

    fn item(remote_id: &str, parent_remote_id: &str, kind: RemoteItemKind) -> RemoteItem {
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

    fn storage_with_root(baseline: &[RemoteItem], fence: &str) -> (Storage, SyncRoot) {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = ProviderAccount::new(provider.clone(), "subject", None, None).unwrap();

        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "root-one",
            provider,
            account.subject,
            "/tmp/root-one",
            Some("configured-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();

        storage.insert_sync_root(&root).unwrap();
        storage
            .begin_sync_root_remote_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(&root.id, baseline, 3)
            .unwrap();
        storage
            .commit_sync_root_remote_inventory_snapshot(
                &root.id,
                &ChangeCursor::new(fence).unwrap(),
                4,
            )
            .unwrap();

        (storage, root)
    }

    #[test]
    fn executor_hydrates_then_observes_later_delete_in_same_batch() {
        let (mut storage, root) = storage_with_root(&[], "fence");

        let folder = item("folder", "canonical-root", RemoteItemKind::Folder);
        let child = item("child", "folder", RemoteItemKind::File);

        let mut provider = FakeProvider::new("canonical-root");
        provider.hydrations.insert(
            folder.remote_id.clone(),
            vec![folder.clone(), child.clone()],
        );

        let changes = vec![
            RemoteChange::Upsert(folder.clone()),
            RemoteChange::Delete {
                remote_id: child.remote_id.clone(),
            },
        ];

        let result = execute_selected_root_change_batch(
            &provider,
            &mut storage,
            &root,
            &ChangeCursor::new("fence").unwrap(),
            &changes,
            &ChangeCursor::new("checkpoint").unwrap(),
            10,
        )
        .unwrap();

        assert_eq!(
            result,
            SelectedRootBatchExecution {
                provider_changes: 2,
                storage_mutations: 3,
                authoritative_items: 1,
                hydrated_items: 2,
                root_revalidations: 0,
                completed_initial_catchup: true,
            }
        );

        assert_eq!(
            storage.list_sync_root_remote_items(&root.id).unwrap(),
            vec![folder]
        );
        assert_eq!(
            storage
                .sync_root_change_cursor(&root.id)
                .unwrap()
                .unwrap()
                .as_str(),
            "checkpoint"
        );
        assert!(
            storage
                .sync_root_remote_inventory_state(&root.id)
                .unwrap()
                .catchup_complete
        );
    }

    #[test]
    fn executor_rejects_incomplete_projection_without_advancing_cursor() {
        let (mut storage, root) = storage_with_root(&[], "fence");
        let provider = FakeProvider::new("canonical-root");

        let orphan = item("orphan", "missing-parent", RemoteItemKind::File);

        let error = execute_selected_root_change_batch(
            &provider,
            &mut storage,
            &root,
            &ChangeCursor::new("fence").unwrap(),
            &[RemoteChange::Upsert(orphan)],
            &ChangeCursor::new("must-not-persist").unwrap(),
            10,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            SelectedRootExecutorError::Projection(
                RootCatalogProjectionError::IncompleteProjectedCatalog
            )
        ));
        assert!(storage.sync_root_change_cursor(&root.id).unwrap().is_none());
        assert!(
            !storage
                .sync_root_remote_inventory_state(&root.id)
                .unwrap()
                .catchup_complete
        );
        assert_eq!(
            storage.list_sync_root_remote_items(&root.id).unwrap(),
            Vec::<RemoteItem>::new()
        );
    }

    #[test]
    fn executor_root_revalidation_failure_prevents_commit() {
        let baseline = item("file", "canonical-root", RemoteItemKind::File);
        let (mut storage, root) = storage_with_root(std::slice::from_ref(&baseline), "fence");

        let mut provider = FakeProvider::new("canonical-root");
        provider.fail_revalidation = true;

        let error = execute_selected_root_change_batch(
            &provider,
            &mut storage,
            &root,
            &ChangeCursor::new("fence").unwrap(),
            &[RemoteChange::Delete {
                remote_id: "canonical-root".into(),
            }],
            &ChangeCursor::new("must-not-persist").unwrap(),
            10,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            SelectedRootExecutorError::ProviderOperationFailed
        ));
        assert!(storage.sync_root_change_cursor(&root.id).unwrap().is_none());
        assert!(
            !storage
                .sync_root_remote_inventory_state(&root.id)
                .unwrap()
                .catchup_complete
        );
        assert_eq!(
            storage.list_sync_root_remote_items(&root.id).unwrap(),
            vec![baseline]
        );
    }

    #[test]
    fn executor_move_out_deletes_existing_subtree_atomically() {
        let folder = item("folder", "canonical-root", RemoteItemKind::Folder);
        let child = item("child", "folder", RemoteItemKind::File);
        let (mut storage, root) = storage_with_root(&[folder.clone(), child], "fence");
        let provider = FakeProvider::new("canonical-root");

        let moved_out = RemoteItem {
            parent_remote_id: Some("outside".into()),
            ..folder
        };

        let result = execute_selected_root_change_batch(
            &provider,
            &mut storage,
            &root,
            &ChangeCursor::new("fence").unwrap(),
            &[RemoteChange::Upsert(moved_out)],
            &ChangeCursor::new("checkpoint").unwrap(),
            10,
        )
        .unwrap();

        assert_eq!(result.authoritative_items, 0);
        assert_eq!(result.storage_mutations, 1);
        assert!(
            storage
                .list_sync_root_remote_items(&root.id)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            storage
                .sync_root_change_cursor(&root.id)
                .unwrap()
                .unwrap()
                .as_str(),
            "checkpoint"
        );
    }
}
