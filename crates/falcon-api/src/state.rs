use falcon_core::Node;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub node: Arc<Node>,
}
