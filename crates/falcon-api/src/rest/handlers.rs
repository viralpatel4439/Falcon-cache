use crate::state::AppState;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

#[derive(Serialize)]
pub struct KeyspaceHealth {
    pub name: String,
    pub ttl_tracked_keys: u64,
    /// Hit-rate, size, and eviction counts — the cost story the UI renders.
    pub cache: CacheHealth,
}

/// What a bounded RAM cache is actually judged on: how often it answers, how
/// much memory it is holding, and how hard it is having to evict.
#[derive(Serialize)]
pub struct CacheHealth {
    pub hit_rate: f64,
    pub hits: u64,
    pub misses: u64,
    pub keys: u64,
    pub bytes: u64,
    /// Entries dropped to stay within the memory budget. Sustained growth here
    /// means the working set does not fit in `capacity_mb`.
    pub evictions: u64,
    /// Entries dropped because their TTL passed.
    pub expired: u64,
}

#[derive(Serialize)]
pub struct FeatureSet {
    pub auth: bool,
    pub wire_protocol: bool,
    pub tls: bool,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    /// Product name.
    pub product: &'static str,
    pub node_id: String,
    pub region: String,
    /// Exactly which optional features are active. Anything false costs
    /// zero at runtime (skip-if-not-configured).
    pub features: FeatureSet,
    pub keyspaces: Vec<KeyspaceHealth>,
}

/// The embedded UI — one self-contained page, no external assets and no build
/// step.
pub async fn dashboard() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("../../assets/ui_cache.html"),
    )
}

/// Prometheus text-format metrics. Unauthenticated by design (like `/healthz`)
/// so scrapers/probes work without a key — put the deployment behind network
/// policy if the numbers are sensitive.
pub async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.node.metrics().encode_prometheus(),
    )
}

/// Readiness probe, distinct from liveness (`/healthz`). Returns 200 only when
/// the node has finished startup. k8s routes traffic on readiness but only
/// restarts on liveness.
pub async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    if state.node.metrics().ready.get() == 1 {
        (axum::http::StatusCode::OK, "ready")
    } else {
        (axum::http::StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

pub async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = state.node.config();
    let keyspaces = state
        .node
        .keyspace_names()
        .filter_map(|name| state.node.keyspace(name).map(|ks| (name.to_string(), ks)))
        .map(|(name, ks)| {
            let s = ks.stats();
            KeyspaceHealth {
                name,
                ttl_tracked_keys: ks.tracked_ttl_keys() as u64,
                cache: CacheHealth {
                    hit_rate: s.hit_rate(),
                    hits: s.hits,
                    misses: s.misses,
                    keys: s.keys,
                    bytes: s.bytes,
                    evictions: s.evictions,
                    expired: s.expired,
                },
            }
        })
        .collect();

    Json(HealthResponse {
        status: "ok",
        product: "Falcon Cache",
        node_id: cfg.node.id.clone(),
        region: cfg.node.region.clone(),
        features: FeatureSet {
            auth: cfg.auth.is_enabled(),
            wire_protocol: cfg.wire.enabled,
            tls: cfg.tls.is_enabled(),
        },
        keyspaces,
    })
}

/// `/health` — the same payload as `/healthz`, exposed under the name the CLI
/// and UI fetch. Kept as a thin alias so both paths stay in sync.
pub async fn health(state: State<AppState>) -> impl IntoResponse {
    healthz(state).await
}
