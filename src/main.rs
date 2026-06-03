use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::response::Response;
use futures_util::{SinkExt, StreamExt};
use uuid::Uuid;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::interceptor::registry::Registry;
use webrtc::api::media_engine::{MediaEngine, MIME_TYPE_VP8};
use webrtc::api::APIBuilder;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::rtp_transceiver::rtp_codec::{RTCRtpCodecCapability};
use webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP;

use crate::actor::{Actor, Addr};
use crate::room::{Room, RoomMessage};
use crate::user::User;
pub mod actor;
pub mod error;
pub mod room;
pub mod user;

use axum::{
    routing::get,
    response::Html,
    Router,
    extract::{State, WebSocketUpgrade},
    extract::ws::WebSocket,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter("webrtc=ERROR,webrtc_ice=ERROR")
        .init();
    let room_actor = Room::default().start();
    let app = Router::new()
        .route("/", get(|| async { 
            Html(std::fs::read_to_string("index.html").unwrap_or_else(|_| "index.html not found".to_string())) 
        }))
        .route("/ws", get(ws_handler))
        .with_state(room_actor);
    // 3. Запускаем сервер Axum на одном порту 8080
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    println!("🚀 Единый SFU Сервер запущен на http://localhost:8080");
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
    m.register_default_codecs()?;
    let registry = register_default_interceptors(Registry::new(), &mut m)?;
    let mut system_engine = SettingEngine::default();
    system_engine
        .set_interface_filter(
            Box::new(|iface|{
                !iface.starts_with("docker") && !iface.starts_with("br-") && !iface.starts_with("veth")
            })
        );
    system_engine.set_ice_timeouts(Some(Duration::from_secs(3)), Some(Duration::from_secs(10)), Some(Duration::from_secs(2)));
    system_engine.disable_srtcp_replay_protection(true);
    system_engine.disable_srtp_replay_protection(true);
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
            },
            RTCIceServer {
                urls: vec![
                    "turn:192.168.0.106:3478".to_string(),
                ],
                username: "admin".to_string(),
                credential: "secretpassword".to_string()
            }
        ],

        ..Default::default()
    };
    // 3. Создаем PeerConnection
    let peer_connection = Arc::new(api.new_peer_connection(config).await?);
    let output_track = Arc::new(TrackLocalStaticRTP::new(
        RTCRtpCodecCapability {
            mime_type: MIME_TYPE_VP8.to_owned(),
            ..Default::default()
        },
        "video".to_owned(),
        peer_id.to_string(),
    ));
    let room_addr = room.clone();
    peer_connection.on_track(Box::new(move |track, _, _| {
        let room_addr = room_addr.clone();
        Box::pin(async move {
            let (tx, rx) = tokio::sync::broadcast::channel(128);
            let peer_id = track.stream_id();
            println!("🎥 [SFU] Камера от {peer_id} потекла в хост-трек комнаты!");
            let _ = room_addr.send(RoomMessage::Broadcast { stream: rx, peer_id: peer_id.clone() }).await;
            let mut packet_count = 0;
            loop {
                let r = track.read_rtp().await;
                let packet = match r {
                    Ok((r, _)) => r,
                    Err(e) => panic!("{e}")
                };
                packet_count += 1;
                if packet_count % 1000 == 0 {
                    // Выводим Sequence Number и Timestamp, чтобы проверить, что поток не "замерз"
                    println!(
                        "📥 [МЕДИА-ВХОД] Получено 1000 пакетов от {}. Текущий seq={}, ts={}", 
                        peer_id, packet.header.sequence_number, packet.header.timestamp
                    );
                }
                if let Err(_) = tx.send(packet) {
                    break;
                }
            }
        })
    }));
    let user = User {
        joined_room: false,
        peer_connection: peer_connection.clone(),
        peer_id: peer_id.to_string(),
        room: room.clone(),
        local_track: output_track,
        ws_tx,
        subscriptions: HashMap::default()
    }.start();
    user.add_stream(ws_rx, |msg|
        actor::StreamItem::Next(user::UserMessage::Websocket {
            message: msg.map_err(|e| error::Error::Axum(e))
        }));
    Ok(())
}