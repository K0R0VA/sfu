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
use std::fmt::Debug;
use std::time::Duration;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use webrtc::api::APIBuilder;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::ice_transport::ice_connection_state::RTCIceConnectionState;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::rtp::packet::Packet;
use webrtc::rtp_transceiver::rtp_codec::{RTCRtpHeaderExtensionCapability, RTPCodecType};
use crate::actor::{Actor, Addr};
use crate::error::Error;
use crate::user::{SignalMessage, Target, initiate_ice_restart};


pub type PacketSender = tokio::sync::broadcast::Sender<Packet>;

pub async fn create_peer() -> Result<RTCPeerConnection, Error> {
    let mut m = MediaEngine::default();
    for uri in [
        "urn:ietf:params:rtp-hdrext:sdes:mid",
        "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id",
        "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id",
    ] {
        m.register_header_extension(
            RTCRtpHeaderExtensionCapability {
                uri: uri.to_owned(),
            },
                RTPCodecType::Video,
            None,
        )?;
    }

    m.register_default_codecs()?;
    
// Регистрируем этот кодек на прием и на отправку

    let registry = register_default_interceptors(Registry::new(), &mut m)?;
    let mut system_engine = SettingEngine::default();
    system_engine
        .set_interface_filter(
            Box::new(|iface|{
                !iface.starts_with("docker") && !iface.starts_with("br-") && !iface.starts_with("veth")
            })
        );
    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_interceptor_registry(registry)
        .with_setting_engine(system_engine)
        .build();
    let config = RTCConfiguration {
        ice_servers: vec![
            RTCIceServer {
                urls: vec![
                    "stun:stun.l.google.com:19302".to_string(),
                ],
                ..Default::default()
            }
        ],
        ..Default::default()
    };
    let peer = api.new_peer_connection(config.clone()).await?;
    Ok(peer)
}

pub trait SyncChannel: Send + 'static {
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

pub trait IceRestartExt: Actor where Self::Message: From<RTCIceConnectionState> {
    const TARGET: Target;
    const CHECK_ICE_STATE: Self::Message;
    fn peer_connection(&self) -> &RTCPeerConnection;
    fn send_target_message(&self, message: SignalMessage) -> impl Future<Output = Result<(), Error>>;
    fn on_reconnect(&self) -> impl Future<Output = Result<(), Error>>;
    fn retry_connect_attempts(&mut self) -> &mut u8;
    fn disconnected(&mut self) -> &mut bool;
    fn on_ice_connection_state_change(&self, addr: Addr<Self>) {
        let pc = self.peer_connection();
        pc.on_ice_connection_state_change(Box::new({
            let addr = addr.clone();
            move |state| {
                let addr = addr.clone();
                Box::pin(async move {
                    let _ = addr.send(state.into()).await;
                })
            }
        }));
    }
    #[allow(async_fn_in_trait)]
    async fn handle_ice_state_change(&mut self, addr: &Addr<Self>, state: RTCIceConnectionState) -> Result<(), Error> {
        match state {
            RTCIceConnectionState::Failed => {
                *self.disconnected() = true;
                let pc = self.peer_connection();        
                let message = initiate_ice_restart(pc, Self::TARGET).await?;
                self.send_target_message(message).await?;
                *self.retry_connect_attempts() += 1;
                addr.send(Self::CHECK_ICE_STATE).await?;
            },
            RTCIceConnectionState::Disconnected => {
                *self.disconnected() = true;
            }
            RTCIceConnectionState::Connected if *self.disconnected() => {
                self.on_reconnect().await?;    
                *self.disconnected() = false;
                *self.retry_connect_attempts() = 0;
            },
            _ => {}
        }
        Ok(())
    }
    #[allow(async_fn_in_trait)]
    async fn check_ice_state(&mut self, ctx: &mut actor::Ctx<'_, Self>) -> Result<(), Error> {
        let current_state = self.peer_connection().ice_connection_state();
        if current_state == RTCIceConnectionState::Failed {
            if *self.retry_connect_attempts() < 5 {
                let addr = ctx.addr.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let _ = addr.send(current_state.into()).await;
                });
            } else {
                self.stop(ctx).await;
            }
        }
        Ok(())
    }
}

#[derive(Default, Debug)]
pub struct CurrentStats {
    packet_loss: f64,
    bitrate_bps: u64,
    last_packets_received: u64,
    last_bytes_received: u64,
    last_nack_count: u64,
}