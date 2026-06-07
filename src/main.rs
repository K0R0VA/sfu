use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use axum::response::Response;
use futures_util::{SinkExt, StreamExt};
use tower_http::services::ServeDir;
use uuid::Uuid;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::interceptor::registry::Registry;
use webrtc::api::media_engine::{MediaEngine};
use webrtc::api::APIBuilder;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use webrtc::rtp::packet::Packet;
use webrtc::rtp_transceiver::rtp_codec::{RTCRtpHeaderExtensionCapability, RTPCodecType};
use webrtc::track::track_remote::TrackRemote;

use crate::actor::{Actor, Addr};
use crate::error::Error;
use crate::room::{PeerStream, Room, RoomMessage, StreamQuality};
use crate::user::{User};
pub mod actor;
pub mod error;
pub mod room;
pub mod user;
pub mod video_subscription;
pub mod quality_monitor;

use axum::{
    routing::get,
    Router,
    extract::{State, WebSocketUpgrade},
    extract::ws::WebSocket,
};

#[tokio::main(flavor = "multi_thread", worker_threads = 24)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter("webrtc=ERROR,webrtc_ice=ERROR,webrtc_template=INFO")
        .init();
    let room_actor = Room::default().start();
    let app = Router::new()
        .fallback_service(
            ServeDir::new("front/dist")
                .append_index_html_on_directories(true)
        )
        .route("/ws", get(ws_handler))
        .with_state(room_actor);
    // 3. Запускаем сервер Axum на одном порту 8080
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!("🚀 Единый SFU Сервер запущен на http://localhost:8080");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(room): State<Addr<Room>>,
) -> Response {
    ws.on_upgrade(|socket| {
        connect_websocket(socket, room)
    })
}

async fn connect_websocket(ws: WebSocket,
    room: Addr<Room>) {
        if let Err(e) = try_connect_websocket(ws, room).await {
            tracing::error!("websocket failed {e}");
        }
    }

async fn try_connect_websocket(
    ws: WebSocket,
    room: Addr<Room>
) -> Result<(), error::Error> {
    let peer_id = Uuid::new_v4();
    let (mut ws_tx, ws_rx) = ws.split();
    let welcome_payload = serde_json::json!({
        "type": "welcome",
        "assigned_peer_id": peer_id
    });
    ws_tx.send(serde_json::to_string(&welcome_payload)?.into()).await?;

    // 1. Инициализация MediaEngine (кодеки)
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
    system_engine.set_receive_mtu(1500);
    // Настраиваем API WebRTC
    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_interceptor_registry(registry)
        .with_setting_engine(system_engine)
        .build();
    // 2. STUN/TURN Сервера (Проблемное место #1: NAT Traversal)
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
    // 3. Создаем PeerConnection
    let peer_connection = Arc::new(api.new_peer_connection(config).await?);
    let user = User {
        peer_connection: peer_connection.clone(),
        peer_id: peer_id,
        room: room.clone(),
        ws_tx,
        subscriptions: HashMap::new()
    }.start();
    user.add_stream(ws_rx, |msg|
        actor::StreamItem::Next(user::UserMessage::Websocket {
            message: msg.map_err(|e| error::Error::Axum(e))
        }));
    let _ = room.send(RoomMessage::Join { peer_id, addr: user }).await;
    let signaler = peer_connection.clone();
    peer_connection.on_track(Box::new(move |track, _, _| {
        let room = room.clone();
        let signaler = signaler.clone();
        let sscr = track.ssrc();
        let mime_type = track.codec().capability.mime_type;
        let kind = track.kind();
        let rid = StreamQuality::from_str(track.rid());
        tokio::spawn(async move {
            loop {
                if let Err(e) = signaler.write_rtcp(&[Box::new(PictureLossIndication {
                    media_ssrc: sscr,
                    sender_ssrc: 0
                })]).await {
                    tracing::error!("[SFU] send PictureLossIndication failed {e}");
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
        Box::pin(async move {
            tracing::info!("[SFU] {kind} от {peer_id} подключен");
            let (tx, stream) = tokio::sync::broadcast::channel(32);
            let active_receiver_counter =  Arc::new(AtomicUsize::new(0));
            let stream = Arc::new(stream);
            let packet_subscription = PacketSubscription {  active_receiver_counter: active_receiver_counter.clone(), stream};
            let mesage = match kind {
                RTPCodecType::Audio | RTPCodecType::Unspecified => RoomMessage::AddAudioTrack { 
                    stream: PeerStream {
                        mime_type,
                        packet_subscription,
                        subscribers: HashSet::new()
                    }, 
                    peer_id 
                },
                RTPCodecType::Video => {
                    let quality = rid.unwrap_or(StreamQuality::High);
                    RoomMessage::AddVideoTrack { 
                        stream: PeerStream {
                            mime_type,
                            packet_subscription,
                            subscribers: HashSet::new()
                        }, 
                        peer_id,
                        quality
                    }
                }   
            };
            let _ = room.send(mesage).await;
            tokio::spawn(async move {
                if let Err(e) = forward_rtp_packets(&track, &tx, &active_receiver_counter).await {
                    tracing::error!("[SFU] forward_rtp_packets failed {e}");
                }
            });
        })
    }));
    Ok(())
}

async fn forward_rtp_packets(track: &TrackRemote, channel: &PacketSender, active_receiver_counter: &AtomicUsize) -> Result<(), Error> {
    loop {
        let (packet, _) = track.read_rtp().await?;
        let active_receiver_count = active_receiver_counter.load(Ordering::Relaxed);
        if active_receiver_count == 0 {
            continue; 
        }
        channel.send(packet).map_err(|_| Error::SystemError { message: "failed send packet".into() })?;
    }
}

pub type PacketStream = Arc<tokio::sync::broadcast::Receiver<Packet>>;
pub type PacketSender = tokio::sync::broadcast::Sender<Packet>;

pub type ActiveReceiverCounter = Arc<AtomicUsize>;

pub struct PacketSubscription {
    pub stream: PacketStream,
    pub active_receiver_counter: ActiveReceiverCounter
}

impl Clone for PacketSubscription {
    fn clone(&self) -> Self {
        Self {
            active_receiver_counter: self.active_receiver_counter.clone(),
            stream: self.stream.clone()
        }
    }
}