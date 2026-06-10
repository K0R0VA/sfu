use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use axum::response::Response;
use futures_util::{StreamExt};
use tower_http::services::ServeDir;
use webrtc::rtp::packet::Packet;
use webrtc::track::track_remote::TrackRemote;
use crate::actor::{Actor, Addr};
use crate::error::Error;
use crate::pli_sender::PliSender;
use crate::room::{Room, RoomMessage};
use crate::user::{User};
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
// pub mod rtp_packet_forwarder;

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

#[derive(Clone)]
pub struct PacketVideoSubscription {
    pub stream: PacketStream,
    pub pli_sender: Addr<PliSender>,
    pub active_receiver_counter: ActiveReceiverCounter
}

#[derive(Clone)]
pub struct PacketAudioSubscription {
    pub stream: PacketStream,
    pub active_receiver_counter: ActiveReceiverCounter
}
