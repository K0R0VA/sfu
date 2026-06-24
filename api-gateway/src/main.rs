use axum::{Json, extract::{Path, ws::Message}, response::{ Response}, routing::post};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use serde::{Deserialize, Serialize};
use sfu::{SyncChannel, actor::{self, Actor, Addr}, server::{Server, ServerMessage}, user::{SignalMessage, SyncMessage, User, UserMessage::{self}}};
use tower_http::services::{ServeDir, ServeFile};

use axum::{
    routing::get,
    Router,
    extract::{State, WebSocketUpgrade},
    extract::ws::WebSocket,
};
use uuid::Uuid;

#[tokio::main(flavor = "multi_thread", worker_threads = 24)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter("webrtc=ERROR,webrtc_ice=ERROR,api_gateway=INFO,sfu=INFO")
        .init();
    let room_actor = Server::default().start();
    let api_router = Router::new()
        .route("/rooms", get(rooms))
        .route("/room", post(create_room))
        .route("/room/{room_id}", get(ws_handler))
        .with_state(room_actor);
    let app = Router::new()
        .nest_service("/assets", ServeDir::new("front/dist/assets"))
        .fallback_service(ServeFile::new("front/dist/index.html"))
        .nest("/api", api_router);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!("🚀 Единый SFU Сервер запущен на http://localhost:8080");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn ws_handler(
    room_id: Path<Uuid>,
    ws: WebSocketUpgrade,
    State(server): State<Addr<Server<WebsocketSync>>>,
) -> Response {
    let room_id = room_id.0;
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = try_connect_websocket(room_id, socket, server).await {
            tracing::error!("websocket failed {e}");
        }
    })
}

async fn rooms(
    State(server): State<Addr<Server<WebsocketSync>>>,
) -> Response {
    let rooms = match server.get_rooms().await {
        Ok(rooms) => rooms,
        Err(e) => return Response::new(e.to_string().into())
    };
    Response::new(serde_json::to_string(&rooms).unwrap().into())
}

#[derive(Deserialize)]
pub struct CreateRoomRequest {
    name: String
}

#[derive(Serialize)]
pub struct CreateRoomResponse {
    id: Uuid
}

async fn create_room(
    State(server): State<Addr<Server<WebsocketSync>>>,
    Json(CreateRoomRequest { name }): Json<CreateRoomRequest>,
) -> Response {
    let room_id = match server.create_room(name).await {
        Ok((room_id, _)) => CreateRoomResponse { id: room_id },
        Err(e) => return Response::new(e.to_string().into())
    };
    Response::new(serde_json::to_string(&room_id).unwrap().into())
}


async fn try_connect_websocket(
    room_id: Uuid,
    ws: WebSocket,
    server: Addr<Server<WebsocketSync>>
) -> Result<(), sfu::error::Error> {
    let (ws_tx, ws_rx) = ws.split();
    let mut sync_channel = WebsocketSync {socket: ws_tx};
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = server.send(ServerMessage::GetRoomAddr { room_id, response_channel: tx }).await;
    let (room_name, room) = match rx.await? {
        Some(room) => room,
        None => {
            sync_channel.send("Room not found".into()).await?;
            return Ok(());
        }
    };
    let message = serde_json::to_string(&SignalMessage::RoomInfo { name: room_name })?;
    sync_channel.send(message).await?;
    let user = User::new(sync_channel, room.clone()).await?;
    let user_id = user.peer_id;
    let addr = user.start();
    addr.add_stream(ws_rx, |msg| {
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
    let _ = server.send(ServerMessage::JoinRoom { room_id, user_id, addr }).await;
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