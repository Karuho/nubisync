use crate::{
    LocalTreeEntry, LocalTreeEntryKind, ReceiveOnlyMaterializationPlanError,
    build_remote_expected_paths, plan_receive_only_materialization, validate_safe_relative_path,
};
use nubisync_core::{RemoteItem, RemoteItemKind};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiveOnlyReceiptState {
    Current,
    Stale,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ReceiveOnlyOwnershipReceipt {
    remote_id: String,
    relative_path: String,
    kind: LocalTreeEntryKind,
    state: ReceiveOnlyReceiptState,
}

impl ReceiveOnlyOwnershipReceipt {
    pub fn new(
        remote_id: impl Into<String>,
        relative_path: impl Into<String>,
        kind: LocalTreeEntryKind,
        state: ReceiveOnlyReceiptState,
    ) -> Result<Self, ReceiveOnlyConvergencePlanError> {
        let remote_id = remote_id.into();
        let relative_path = relative_path.into();

        if remote_id.trim().is_empty() {
            return Err(ReceiveOnlyConvergencePlanError::InvalidReceiptRemoteId);
        }

        validate_safe_relative_path(&relative_path)
            .map_err(|_| ReceiveOnlyConvergencePlanError::InvalidReceiptPath)?;

        Ok(Self {
            remote_id,
            relative_path,
            kind,
            state,
        })
    }

    pub fn remote_id(&self) -> &str {
        &self.remote_id
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    pub fn kind(&self) -> LocalTreeEntryKind {
        self.kind
    }

    pub fn state(&self) -> ReceiveOnlyReceiptState {
        self.state
    }
}

impl std::fmt::Debug for ReceiveOnlyOwnershipReceipt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReceiveOnlyOwnershipReceipt")
            .field("remote_id", &"[redacted]")
            .field("relative_path", &"[redacted]")
            .field("kind", &self.kind)
            .field("state", &self.state)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReceiveOnlyConvergenceActionKind {
    CreateDirectory,
    MaterializeMissingFile,
    VerifyExistingFile,
    RevalidateStaleFileReplacement,
    RevalidateStaleFileDeletion,
    DeleteOwnedEmptyDirectory,
    Blocked,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ReceiveOnlyConvergenceAction {
    kind: ReceiveOnlyConvergenceActionKind,
    remote_id: Option<String>,
    relative_path: String,
    depth: usize,
}

impl ReceiveOnlyConvergenceAction {
    pub fn kind(&self) -> ReceiveOnlyConvergenceActionKind {
        self.kind
    }

    pub fn remote_id(&self) -> Option<&str> {
        self.remote_id.as_deref()
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    pub fn depth(&self) -> usize {
        self.depth
    }
}

impl std::fmt::Debug for ReceiveOnlyConvergenceAction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReceiveOnlyConvergenceAction")
            .field("kind", &self.kind)
            .field("remote_id", &self.remote_id.as_ref().map(|_| "[redacted]"))
            .field("relative_path", &"[redacted]")
            .field("depth", &self.depth)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiveOnlyConvergencePlan {
    actions: Vec<ReceiveOnlyConvergenceAction>,
    pub remote_items: usize,
    pub local_entries: usize,
    pub receipt_count: usize,
    pub create_directories: usize,
    pub materialize_missing_files: usize,
    pub verify_existing_files: usize,
    pub revalidate_stale_file_replacements: usize,
    pub revalidate_stale_file_deletions: usize,
    pub delete_owned_empty_directories: usize,
    pub blocked_actions: usize,
    pub current_owned_files: usize,
    pub current_owned_directories: usize,
    pub unowned_matching_directories: usize,
}

impl ReceiveOnlyConvergencePlan {
    pub fn actions(&self) -> &[ReceiveOnlyConvergenceAction] {
        &self.actions
    }

    pub fn action_count(&self) -> usize {
        self.actions.len()
    }

    pub fn blocked(&self) -> bool {
        self.blocked_actions != 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiveOnlyConvergencePlanError {
    Materialization(ReceiveOnlyMaterializationPlanError),
    InvalidMaxActions,
    InvalidReceiptRemoteId,
    InvalidReceiptPath,
    DuplicateReceiptRemoteId,
    DuplicateReceiptPath,
    ActionLimitExceeded,
}

impl From<ReceiveOnlyMaterializationPlanError> for ReceiveOnlyConvergencePlanError {
    fn from(value: ReceiveOnlyMaterializationPlanError) -> Self {
        Self::Materialization(value)
    }
}

#[derive(Clone)]
struct ActionDraft {
    kind: ReceiveOnlyConvergenceActionKind,
    remote_id: Option<String>,
    relative_path: String,
}

pub fn plan_receive_only_convergence(
    remote_items: &[RemoteItem],
    local_entries: &[LocalTreeEntry],
    receipts: &[ReceiveOnlyOwnershipReceipt],
    max_actions: usize,
) -> Result<ReceiveOnlyConvergencePlan, ReceiveOnlyConvergencePlanError> {
    if max_actions == 0 {
        return Err(ReceiveOnlyConvergencePlanError::InvalidMaxActions);
    }

    let _ = plan_receive_only_materialization(remote_items, local_entries)?;
    let expected_paths = build_remote_expected_paths(remote_items)?;

    let mut local_by_path = HashMap::with_capacity(local_entries.len());
    for entry in local_entries {
        if local_by_path
            .insert(entry.relative_path().to_owned(), entry.kind)
            .is_some()
        {
            return Err(ReceiveOnlyConvergencePlanError::Materialization(
                ReceiveOnlyMaterializationPlanError::DuplicateLocalPath,
            ));
        }
    }

    let mut receipt_by_remote_id = HashMap::with_capacity(receipts.len());
    let mut receipt_by_path = HashMap::with_capacity(receipts.len());
    for receipt in receipts {
        if receipt_by_remote_id
            .insert(receipt.remote_id(), receipt)
            .is_some()
        {
            return Err(ReceiveOnlyConvergencePlanError::DuplicateReceiptRemoteId);
        }
        if receipt_by_path
            .insert(receipt.relative_path(), receipt)
            .is_some()
        {
            return Err(ReceiveOnlyConvergencePlanError::DuplicateReceiptPath);
        }
    }

    let mut expected_by_remote_id = HashMap::with_capacity(expected_paths.len());
    let mut expected_path_names = HashSet::with_capacity(expected_paths.len());
    for (path, kind, remote_id, _) in &expected_paths {
        expected_by_remote_id.insert(remote_id.as_str(), (path.as_str(), *kind));
        expected_path_names.insert(path.as_str());
    }

    let mut drafts: BTreeMap<String, ActionDraft> = BTreeMap::new();
    let mut current_owned_files = 0_usize;
    let mut current_owned_directories = 0_usize;
    let mut unowned_matching_directories = 0_usize;

    for (path, remote_kind, remote_id, _) in &expected_paths {
        let expected_local_kind = local_kind_for_remote(*remote_kind);
        let receipt_by_id = receipt_by_remote_id.get(remote_id.as_str()).copied();
        let receipt_at_path = receipt_by_path.get(path.as_str()).copied();

        let receipt_identity_conflict = receipt_by_id.is_some_and(|receipt| {
            receipt.relative_path() != path || receipt.kind() != expected_local_kind
        });
        let receipt_path_conflict =
            receipt_at_path.is_some_and(|receipt| receipt.remote_id() != remote_id.as_str());

        if receipt_identity_conflict || receipt_path_conflict {
            set_action(
                &mut drafts,
                ActionDraft {
                    kind: ReceiveOnlyConvergenceActionKind::Blocked,
                    remote_id: Some(remote_id.clone()),
                    relative_path: path.clone(),
                },
                max_actions,
            )?;
            continue;
        }

        match local_by_path.get(path.as_str()).copied() {
            None => {
                let kind = match remote_kind {
                    RemoteItemKind::Folder => ReceiveOnlyConvergenceActionKind::CreateDirectory,
                    RemoteItemKind::File => {
                        ReceiveOnlyConvergenceActionKind::MaterializeMissingFile
                    }
                };
                set_action(
                    &mut drafts,
                    ActionDraft {
                        kind,
                        remote_id: Some(remote_id.clone()),
                        relative_path: path.clone(),
                    },
                    max_actions,
                )?;
            }
            Some(local_kind) if local_kind != expected_local_kind => {
                set_action(
                    &mut drafts,
                    ActionDraft {
                        kind: ReceiveOnlyConvergenceActionKind::Blocked,
                        remote_id: Some(remote_id.clone()),
                        relative_path: path.clone(),
                    },
                    max_actions,
                )?;
            }
            Some(LocalTreeEntryKind::Directory) => match receipt_by_id {
                Some(receipt) if receipt.state() == ReceiveOnlyReceiptState::Current => {
                    current_owned_directories += 1;
                }
                Some(_) => {
                    set_action(
                        &mut drafts,
                        ActionDraft {
                            kind: ReceiveOnlyConvergenceActionKind::Blocked,
                            remote_id: Some(remote_id.clone()),
                            relative_path: path.clone(),
                        },
                        max_actions,
                    )?;
                }
                None => {
                    unowned_matching_directories += 1;
                }
            },
            Some(LocalTreeEntryKind::File) => match receipt_by_id {
                Some(receipt) if receipt.state() == ReceiveOnlyReceiptState::Current => {
                    current_owned_files += 1;
                }
                Some(receipt) if receipt.state() == ReceiveOnlyReceiptState::Stale => {
                    set_action(
                        &mut drafts,
                        ActionDraft {
                            kind: ReceiveOnlyConvergenceActionKind::RevalidateStaleFileReplacement,
                            remote_id: Some(remote_id.clone()),
                            relative_path: path.clone(),
                        },
                        max_actions,
                    )?;
                }
                Some(_) => unreachable!("all receipt states are matched"),
                None => {
                    set_action(
                        &mut drafts,
                        ActionDraft {
                            kind: ReceiveOnlyConvergenceActionKind::VerifyExistingFile,
                            remote_id: Some(remote_id.clone()),
                            relative_path: path.clone(),
                        },
                        max_actions,
                    )?;
                }
            },
        }
    }

    for entry in local_entries {
        let path = entry.relative_path();
        if expected_path_names.contains(path) {
            continue;
        }

        let Some(receipt) = receipt_by_path.get(path).copied() else {
            set_action(
                &mut drafts,
                ActionDraft {
                    kind: ReceiveOnlyConvergenceActionKind::Blocked,
                    remote_id: None,
                    relative_path: path.to_owned(),
                },
                max_actions,
            )?;
            continue;
        };

        if receipt.state() != ReceiveOnlyReceiptState::Stale
            || receipt.kind() != entry.kind
            || expected_by_remote_id.contains_key(receipt.remote_id())
        {
            set_action(
                &mut drafts,
                ActionDraft {
                    kind: ReceiveOnlyConvergenceActionKind::Blocked,
                    remote_id: Some(receipt.remote_id().to_owned()),
                    relative_path: path.to_owned(),
                },
                max_actions,
            )?;
            continue;
        }

        let kind = match entry.kind {
            LocalTreeEntryKind::File => {
                ReceiveOnlyConvergenceActionKind::RevalidateStaleFileDeletion
            }
            LocalTreeEntryKind::Directory if local_directory_is_empty(path, local_entries) => {
                ReceiveOnlyConvergenceActionKind::DeleteOwnedEmptyDirectory
            }
            LocalTreeEntryKind::Directory => ReceiveOnlyConvergenceActionKind::Blocked,
        };

        set_action(
            &mut drafts,
            ActionDraft {
                kind,
                remote_id: Some(receipt.remote_id().to_owned()),
                relative_path: path.to_owned(),
            },
            max_actions,
        )?;
    }

    for receipt in receipts {
        let remote_present = expected_by_remote_id.contains_key(receipt.remote_id());
        let local_present = local_by_path.contains_key(receipt.relative_path());

        if local_present {
            continue;
        }

        let valid_missing_remote_target = remote_present
            && expected_by_remote_id
                .get(receipt.remote_id())
                .is_some_and(|(path, kind)| {
                    *path == receipt.relative_path()
                        && local_kind_for_remote(*kind) == receipt.kind()
                });

        if valid_missing_remote_target {
            continue;
        }

        set_action(
            &mut drafts,
            ActionDraft {
                kind: ReceiveOnlyConvergenceActionKind::Blocked,
                remote_id: Some(receipt.remote_id().to_owned()),
                relative_path: receipt.relative_path().to_owned(),
            },
            max_actions,
        )?;
    }

    let mut actions = drafts
        .into_values()
        .map(|draft| ReceiveOnlyConvergenceAction {
            depth: path_depth(&draft.relative_path),
            kind: draft.kind,
            remote_id: draft.remote_id,
            relative_path: draft.relative_path,
        })
        .collect::<Vec<_>>();

    actions.sort_by(|left, right| {
        action_priority(left.kind)
            .cmp(&action_priority(right.kind))
            .then_with(|| {
                match (
                    left.kind == ReceiveOnlyConvergenceActionKind::DeleteOwnedEmptyDirectory,
                    right.kind == ReceiveOnlyConvergenceActionKind::DeleteOwnedEmptyDirectory,
                ) {
                    (true, true) => right.depth.cmp(&left.depth),
                    _ => left.depth.cmp(&right.depth),
                }
            })
            .then_with(|| left.relative_path.cmp(&right.relative_path))
    });

    let mut plan = ReceiveOnlyConvergencePlan {
        remote_items: remote_items.len(),
        local_entries: local_entries.len(),
        receipt_count: receipts.len(),
        actions,
        create_directories: 0,
        materialize_missing_files: 0,
        verify_existing_files: 0,
        revalidate_stale_file_replacements: 0,
        revalidate_stale_file_deletions: 0,
        delete_owned_empty_directories: 0,
        blocked_actions: 0,
        current_owned_files,
        current_owned_directories,
        unowned_matching_directories,
    };

    for action in &plan.actions {
        match action.kind {
            ReceiveOnlyConvergenceActionKind::CreateDirectory => plan.create_directories += 1,
            ReceiveOnlyConvergenceActionKind::MaterializeMissingFile => {
                plan.materialize_missing_files += 1
            }
            ReceiveOnlyConvergenceActionKind::VerifyExistingFile => plan.verify_existing_files += 1,
            ReceiveOnlyConvergenceActionKind::RevalidateStaleFileReplacement => {
                plan.revalidate_stale_file_replacements += 1
            }
            ReceiveOnlyConvergenceActionKind::RevalidateStaleFileDeletion => {
                plan.revalidate_stale_file_deletions += 1
            }
            ReceiveOnlyConvergenceActionKind::DeleteOwnedEmptyDirectory => {
                plan.delete_owned_empty_directories += 1
            }
            ReceiveOnlyConvergenceActionKind::Blocked => plan.blocked_actions += 1,
        }
    }

    Ok(plan)
}

fn set_action(
    actions: &mut BTreeMap<String, ActionDraft>,
    candidate: ActionDraft,
    max_actions: usize,
) -> Result<(), ReceiveOnlyConvergencePlanError> {
    match actions.get_mut(&candidate.relative_path) {
        Some(existing) if existing.kind == candidate.kind => {}
        Some(existing) => {
            existing.kind = ReceiveOnlyConvergenceActionKind::Blocked;
            existing.remote_id = None;
        }
        None => {
            actions.insert(candidate.relative_path.clone(), candidate);
            if actions.len() > max_actions {
                return Err(ReceiveOnlyConvergencePlanError::ActionLimitExceeded);
            }
        }
    }

    Ok(())
}

fn local_kind_for_remote(kind: RemoteItemKind) -> LocalTreeEntryKind {
    match kind {
        RemoteItemKind::File => LocalTreeEntryKind::File,
        RemoteItemKind::Folder => LocalTreeEntryKind::Directory,
    }
}

fn local_directory_is_empty(path: &str, local_entries: &[LocalTreeEntry]) -> bool {
    let prefix = format!("{path}/");
    !local_entries
        .iter()
        .any(|entry| entry.relative_path().starts_with(&prefix))
}

fn path_depth(path: &str) -> usize {
    path.split('/').count()
}

fn action_priority(kind: ReceiveOnlyConvergenceActionKind) -> u8 {
    match kind {
        ReceiveOnlyConvergenceActionKind::CreateDirectory => 0,
        ReceiveOnlyConvergenceActionKind::MaterializeMissingFile
        | ReceiveOnlyConvergenceActionKind::VerifyExistingFile
        | ReceiveOnlyConvergenceActionKind::RevalidateStaleFileReplacement => 1,
        ReceiveOnlyConvergenceActionKind::RevalidateStaleFileDeletion => 2,
        ReceiveOnlyConvergenceActionKind::DeleteOwnedEmptyDirectory => 3,
        ReceiveOnlyConvergenceActionKind::Blocked => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn remote(remote_id: &str, parent: &str, name: &str, kind: RemoteItemKind) -> RemoteItem {
        RemoteItem {
            remote_id: remote_id.into(),
            parent_remote_id: Some(parent.into()),
            name: name.into(),
            kind,
            size_bytes: (kind == RemoteItemKind::File).then_some(5),
            modified_unix_ms: None,
            trashed: false,
        }
    }

    fn local(path: &str, kind: LocalTreeEntryKind) -> LocalTreeEntry {
        LocalTreeEntry::new(path, kind).unwrap()
    }

    fn receipt(
        remote_id: &str,
        path: &str,
        kind: LocalTreeEntryKind,
        state: ReceiveOnlyReceiptState,
    ) -> ReceiveOnlyOwnershipReceipt {
        ReceiveOnlyOwnershipReceipt::new(remote_id, path, kind, state).unwrap()
    }

    #[test]
    fn multi_item_plan_classifies_receive_only_work_without_mutation() {
        let remote_items = vec![
            remote("docs", "root", "docs", RemoteItemKind::Folder),
            remote("readme", "docs", "readme.txt", RemoteItemKind::File),
            remote("top", "root", "top.txt", RemoteItemKind::File),
        ];
        let local_entries = vec![
            local("docs", LocalTreeEntryKind::Directory),
            local("docs/readme.txt", LocalTreeEntryKind::File),
            local("gone.txt", LocalTreeEntryKind::File),
            local("old", LocalTreeEntryKind::Directory),
        ];
        let receipts = vec![
            receipt(
                "gone",
                "gone.txt",
                LocalTreeEntryKind::File,
                ReceiveOnlyReceiptState::Stale,
            ),
            receipt(
                "old",
                "old",
                LocalTreeEntryKind::Directory,
                ReceiveOnlyReceiptState::Stale,
            ),
        ];

        let plan =
            plan_receive_only_convergence(&remote_items, &local_entries, &receipts, 32).unwrap();

        assert_eq!(plan.action_count(), 4);
        assert_eq!(plan.materialize_missing_files, 1);
        assert_eq!(plan.verify_existing_files, 1);
        assert_eq!(plan.revalidate_stale_file_deletions, 1);
        assert_eq!(plan.delete_owned_empty_directories, 1);
        assert_eq!(plan.unowned_matching_directories, 1);
        assert_eq!(plan.blocked_actions, 0);
    }

    #[test]
    fn create_directories_sort_before_descendant_file_actions() {
        let remote_items = vec![
            remote("parent", "root", "parent", RemoteItemKind::Folder),
            remote("child", "parent", "child", RemoteItemKind::Folder),
            remote("file", "child", "file.txt", RemoteItemKind::File),
        ];

        let plan = plan_receive_only_convergence(&remote_items, &[], &[], 16).unwrap();
        let kinds = plan
            .actions()
            .iter()
            .map(ReceiveOnlyConvergenceAction::kind)
            .collect::<Vec<_>>();

        assert_eq!(
            kinds,
            vec![
                ReceiveOnlyConvergenceActionKind::CreateDirectory,
                ReceiveOnlyConvergenceActionKind::CreateDirectory,
                ReceiveOnlyConvergenceActionKind::MaterializeMissingFile,
            ]
        );
        assert!(plan.actions()[0].depth() < plan.actions()[1].depth());
    }

    #[test]
    fn non_empty_stale_owned_directory_is_blocked() {
        let local_entries = vec![
            local("old", LocalTreeEntryKind::Directory),
            local("old/keep.txt", LocalTreeEntryKind::File),
        ];
        let receipts = vec![receipt(
            "old",
            "old",
            LocalTreeEntryKind::Directory,
            ReceiveOnlyReceiptState::Stale,
        )];

        let plan = plan_receive_only_convergence(&[], &local_entries, &receipts, 16).unwrap();
        assert_eq!(plan.delete_owned_empty_directories, 0);
        assert_eq!(plan.blocked_actions, 2);
    }

    #[test]
    fn action_limit_fails_closed() {
        let remote_items = vec![
            remote("one", "root", "one", RemoteItemKind::Folder),
            remote("two", "root", "two", RemoteItemKind::Folder),
        ];

        assert_eq!(
            plan_receive_only_convergence(&remote_items, &[], &[], 1),
            Err(ReceiveOnlyConvergencePlanError::ActionLimitExceeded)
        );
    }

    #[test]
    fn debug_redacts_receipt_and_action_identity() {
        let receipt = receipt(
            "secret-id",
            "secret/path",
            LocalTreeEntryKind::File,
            ReceiveOnlyReceiptState::Stale,
        );
        let action = ReceiveOnlyConvergenceAction {
            kind: ReceiveOnlyConvergenceActionKind::RevalidateStaleFileDeletion,
            remote_id: Some("secret-id".into()),
            relative_path: "secret/path".into(),
            depth: 2,
        };

        let receipt_debug = format!("{receipt:?}");
        let action_debug = format!("{action:?}");

        assert!(!receipt_debug.contains("secret-id"));
        assert!(!receipt_debug.contains("secret/path"));
        assert!(!action_debug.contains("secret-id"));
        assert!(!action_debug.contains("secret/path"));
    }
}
