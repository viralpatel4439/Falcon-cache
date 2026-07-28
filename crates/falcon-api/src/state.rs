use falcon_core::Node;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub node: Arc<Node>,
    /// Path to the profile file, so the UI's `POST /config` can persist edits
    /// through the same CLI/UI-only config path (never env vars).
    pub profile_path: Arc<PathBuf>,
}
