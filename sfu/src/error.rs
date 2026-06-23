use std::borrow::Cow;

use tokio::sync::broadcast::error::RecvError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    WebRtc(#[from] webrtc::Error),
    #[error(transparent)]
    Broadcast(#[from] RecvError),
    #[error(transparent)]
    Oneshot(#[from] tokio::sync::oneshot::error::RecvError),
    #[error("System failed on {message}")]
    SystemError {message: Cow<'static, str> }
}