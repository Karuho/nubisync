//! Synchronization planning primitives.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncMode {
    TwoWay,
    MirrorLocalToRemote,
    ReceiveOnly,
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
