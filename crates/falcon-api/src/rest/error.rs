use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use falcon_core::NodeError;
use serde_json::json;

/// Everything the REST layer can reject a request with.
///
/// There is no `Internal` variant: the cache serves every operation from RAM,
/// so once a request is well-formed and names a real keyspace, it cannot fail.
pub enum ApiError {
    NotFound,
    UnknownKeyspace(String),
    BadRequest(String),
}

impl From<NodeError> for ApiError {
    fn from(e: NodeError) -> Self {
        match e {
            NodeError::UnknownKeyspace(name) => ApiError::UnknownKeyspace(name),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::NotFound => (StatusCode::NOT_FOUND, "key not found".to_string()),
            ApiError::UnknownKeyspace(name) => (
                StatusCode::NOT_FOUND,
                format!("unknown keyspace '{name}'"),
            ),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}
