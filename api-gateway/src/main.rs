use axum::{extract::ws::Message, response::Response};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use sfu::{SyncChannel, actor::{self, Actor, Addr}, room::{Room, RoomMessage}, user::{SyncMessage, User, UserMessage::{self}}};
use tower_http::services::ServeDir;

use axum::{
    routing::get,
    Router,
    extract::{State, WebSocketUpgrade},
    extract::ws::WebSocket,
};

#[tokio::main(flavor = "multi_thread", worker_threads = 24)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter("webrtc=ERROR,webrtc_ice=ERROR,api_gateway=INFO,sfu=INFO")
        .init();
    let room_actor = Room::default().start();
    let app = Router::new()
        .fallback_service(
            ServeDir::new("front/dist")
                .append_index_html_on_directories(true)
        )
        .route("/ws", get(ws_handler))
        .with_state(room_actor);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!("🚀 Единый SFU Сервер запущен на http://localhost:8080");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(room): State<Addr<Room<WebsocketSync>>>,
) -> Response {
    ws.on_upgrade(|socket| {
        connect_websocket(socket, room)
    })
}

async fn connect_websocket(ws: WebSocket,
    room: Addr<Room<WebsocketSync>>) {
        if let Err(e) = try_connect_websocket(ws, room).await {
            tracing::error!("websocket failed {e}");
        }
    }

async fn try_connect_websocket(
    ws: WebSocket,
    room: Addr<Room<WebsocketSync>>
) -> Result<(), sfu::error::Error> {
    let (ws_tx, ws_rx) = ws.split();
    let sync_channel = WebsocketSync {socket: ws_tx};
    let user = User::new(sync_channel, room.clone()).await?;
    let peer_id = user.peer_id;
    let user = user.start();
    user.add_stream(ws_rx, |msg| {
        let message = match msg {
            Ok(Message::Text(text)) => {
                let message = match serde_json::from_slice(text.as_bytes()) {
                    Ok(msg) => msg,
                    Err(e) => {
                        tracing::error!("Failed parse websocket message {e}");
                        return actor::StreamItem::Close
                    }
                };
                SyncMessage::Message(message)
            },
            Ok(Message::Close(_) ) => SyncMessage::Close,
            Err(e) => SyncMessage::Error(e.to_string()),
            _ => return actor::StreamItem::Skip
        };
        actor::StreamItem::Next(UserMessage::SyncMessage(message)) 
    });
    let _ = room.send(RoomMessage::Join { peer_id, addr: user }).await;
    Ok(())
}

pub struct WebsocketSync {
    socket: SplitSink<WebSocket, Message>
}

impl SyncChannel for WebsocketSync {
    type Item = String;
    async fn send(&mut self, message: String) -> Result<(), sfu::error::Error> {
        self.socket.send(Message::Text(message.into()))
            .await
            .map_err(|e| sfu::error::Error::SystemError { message: e.to_string().into() })?;
        Ok(())
    }
}