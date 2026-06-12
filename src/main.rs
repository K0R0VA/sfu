use axum::response::Response;
use futures_util::{StreamExt};
use tower_http::services::ServeDir;
use webrtc::rtp::packet::Packet;
use crate::actor::{Actor, Addr};
use crate::audio_packet_forwarder::AudioPacketForwarder;
use crate::pli_sender::PliSender;
use crate::room::{Room, RoomMessage};
use crate::rtp_packet_forwarder::RtpPacketForwarder;
use crate::user::{User};
use crate::video_packet_forwarder::VideoPacketForwarder;
pub mod actor;
pub mod error;
pub mod room;
pub mod user;
pub mod video_subscription;
pub mod quality_monitor;
pub mod video_packet_forwarder;
pub mod audio_subscription;
pub mod audio_packet_forwarder;
pub mod pli_sender;
pub mod rtp_packet_forwarder;
pub mod subscriber_pc;
pub mod publisher_pc;

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
    let (ws_tx, ws_rx) = ws.split();
    let user = User::new(ws_tx, room.clone()).await?;
    let peer_id = user.peer_id;
    let user = user.start();
    user.add_stream(ws_rx, |msg|
        actor::StreamItem::Next(user::UserMessage::Websocket {
            message: msg.map_err(|e| error::Error::Axum(e))
        }));
    let _ = room.send(RoomMessage::Join { peer_id, addr: user }).await;
    Ok(())
}

pub type PacketSender = tokio::sync::broadcast::Sender<Packet>;

#[derive(Clone)]
pub struct PacketVideoSubscription {
    pub pli_sender: Addr<PliSender>,
    pub rtp_packet_forwarder: Addr<RtpPacketForwarder<VideoPacketForwarder>>
}

#[derive(Clone)]
pub struct PacketAudioSubscription {
    pub rtp_packet_forwarder: Addr<RtpPacketForwarder<AudioPacketForwarder>>
}
