#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    WebRtc(#[from] webrtc::Error),
    #[error(transparent)]
    Axum(#[from] axum::Error)
}