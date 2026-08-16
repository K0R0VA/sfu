pub mod actor;
pub mod error;
pub mod room;
pub mod user;
pub mod video_layer_manager;
pub mod quality_monitor;
pub mod video_packet_forwarder;
pub mod audio_packet_forwarder;
pub mod keyframe_interceptor;
pub mod rtp_packet_gateway_router;
pub mod subscriber;
pub mod publisher;
pub mod server;
pub mod simulcast_manager;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use rtc::interceptor::Registry;
use rtc::rtp_transceiver::RTCRtpTransceiverDirection;
use rtc::rtp_transceiver::rtp_sender::{RTCRtpHeaderExtensionCapability, RtpCodecKind};
use uuid::Uuid;
use webrtc::peer_connection::{MediaEngine, PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCConfigurationBuilder, RTCIceServer, register_default_interceptors};
use webrtc::runtime::TokioRuntime;
use crate::error::Error;
use crate::user::{SignalMessage};


pub async fn create_peer(handler: impl PeerConnectionEventHandler, direction: RTCRtpTransceiverDirection) -> Result<Box<dyn PeerConnection>, Error> {
    let mut m = MediaEngine::default();
    m.register_default_codecs()?;
    for uri in [
        "urn:ietf:params:rtp-hdrext:sdes:mid",
        "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id",
        "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id",
    ] {
        m.register_header_extension(
            RTCRtpHeaderExtensionCapability { uri: uri.to_owned() },
            RtpCodecKind::Video,
            Some(direction),
        )?;
    }
    let registry = register_default_interceptors(Registry::new(), &mut m)?;
    let config = RTCConfigurationBuilder::default()
        .with_ice_servers(vec![RTCIceServer {
            urls: vec!["stun:stun.bluesip.net:3478".to_owned()],
            ..Default::default()
        }])
        .build();
    let pc = PeerConnectionBuilder::new()
        .with_media_engine(m)
        .with_interceptor_registry(registry)
        .with_configuration(config)
        .with_runtime(Arc::new(TokioRuntime))
        .with_handler(Arc::new(handler))
        .with_udp_addrs(vec!["0.0.0.0:0"])
        .build()
        .await?;
    Ok(Box::new(pc))
}

pub trait SignalingClient: Send + 'static {
    type Item: From<SignalMessage>;
    type Error: std::error::Error + Debug;
    fn send(&mut self, message: Self::Item) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait Storage: Send + 'static + Sized {
    type Configuration: StorageConfiguration;
    type Error: std::error::Error + Debug;
    fn connect(configuration: &Self::Configuration) -> impl Future<Output = Result<Self, Self::Error>> + Send;
    fn insert(&mut self, item: StorageItem) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait StorageConfiguration: Send +'static + Sized {
    type Error: std::error::Error + Debug;
    fn from_env() -> Result<Self, Self::Error>;
}

pub struct StorageItem<'a> {
    pub stats: &'a CurrentStats,
    pub timestamp: DateTime<Utc>,
    pub connection_id: Uuid
}

#[derive(Debug, Default)]
pub struct CurrentStats {
    // Метрики потерь
    pub packets_received: u64,
    pub packets_lost: u64,
    pub loss_rate: f64, // процент потерь за последний интервал
    
    // Метрики качества видео
    pub frames_decoded: u32,
    pub frames_dropped: u32,
    pub avg_qp: Option<f64>,
    pub jitter: f64, // из RTCReceivedRtpStreamStats
    
    // Метрики стабильности
    pub freeze_count: u32,
    pub total_freezes_duration: f64,
    pub concealment_events: u64,
    
    // История для анализа трендов (опционально)
    pub loss_history: VecDeque<f64>, // последние N значений потерь
}