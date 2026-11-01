pub mod mcp_stdio;
use axum::{routing::post, Router};
use std::sync::{Arc, Mutex};

pub struct ProxyState {}

pub fn build_router(_state: Arc<Mutex<ProxyState>>) -> Router {
    Router::new().route("/v1/chat/completions", post(|| async {}))
}
