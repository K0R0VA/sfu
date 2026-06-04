use std::collections::HashMap;
use std::sync::Arc;
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

use crate::actor::{Actor, Addr};
use crate::room::{Peer, Room, RoomMessage};
use crate::user::{User};
pub mod actor;
pub mod error;
pub mod room;
pub mod user;

use axum::{
    routing::get,
    Router,
    extract::{State, WebSocketUpgrade},
    extract::ws::WebSocket,
};

#[tokio::main(flavor = "multi_thread", worker_threads = 24)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter("webrtc=ERROR,webrtc_ice=ERROR")
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
    
// Регистрируем этот кодек на прием и на отправку
    let registry = register_default_interceptors(Registry::new(), &mut m)?;
    let mut system_engine = SettingEngine::default();
    system_engine
        .set_interface_filter(
            Box::new(|iface|{
                !iface.starts_with("docker") && !iface.starts_with("br-") && !iface.starts_with("veth")
            })
        );
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
    let signaler = peer_connection.clone();
    peer_connection.on_track(Box::new(move |track, _, _| {
        let room = room.clone();
        let user = user.clone();
        let signaler = signaler.clone();
        let sscr = track.ssrc();
        let codec_mime_type = track.codec().capability.mime_type;
        tokio::spawn(async move {
            loop {
                if let Err(e) = signaler.write_rtcp(&[Box::new(PictureLossIndication {
                    media_ssrc: sscr,
                    sender_ssrc: 0
                })]).await {
                    tracing::error!("send PictureLossIndication failed {e}");
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
        Box::pin(async move {
            let (tx, stream) = tokio::sync::broadcast::channel(32);
            println!("🎥 [SFU] Камера от {peer_id} потекла в хост-трек комнаты!");
            let _ = room.send(RoomMessage::Join { peer: Peer {
                codec_mime_type,
                stream,
                user
            }, peer_id }).await;
            tokio::spawn(async move {
                loop {
                    let r = track.read_rtp().await;
                    let packet = match r {
                        Ok((r, _)) => r,
                        Err(e) => {
                            tracing::error!("🎥 [SFU] failed read_rtp {e}");
                            break
                        }
                    };
                    if tx.receiver_count() == 1 { continue; }
                        if let Err(_) = tx.send(packet) {
                            tracing::error!("🎥 [SFU] failed send packet");
                            break;
                        }
                    }
            });
        })
    }));
    
    
    Ok(())
}