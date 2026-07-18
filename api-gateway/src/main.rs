use axum::{Json, extract::{Path, ws::Message}, response::{ Response}, routing::post};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use serde::{Deserialize, Serialize};
use sfu::{Storage, StorageConfiguration, SyncChannel, actor::{self, Actor, Addr}, server::{Server, ServerMessage}, user::{SignalMessage, SyncMessage, User, UserMessage::{self}}};
use tokio::{fs::File, io::AsyncWriteExt};
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
    State(server): State<Addr<Server<WebsocketSyncChannel, FileStorage>>>,
) -> Response {
    let room_id = room_id.0;
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = try_connect_websocket(room_id, socket, server).await {
            tracing::error!("websocket failed {e}");
        }
    })
}

async fn rooms(
    State(server): State<Addr<Server<WebsocketSyncChannel, FileStorage>>>,
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
    State(server): State<Addr<Server<WebsocketSyncChannel, FileStorage>>>,
    Json(CreateRoomRequest { name }): Json<CreateRoomRequest>,
) -> Response {
    let result = server
        .create_room(name).await
        .and_then(|(room_id, _)| Ok(serde_json::to_string(&CreateRoomResponse { id: room_id })?));
    let json = match result {
        Ok(json) => json,
        Err(e) => return Response::new(e.to_string().into())
    };
    Response::new(json.into())
}


async fn try_connect_websocket(
    room_id: Uuid,
    ws: WebSocket,
    server: Addr<Server<WebsocketSyncChannel, FileStorage>>
) -> Result<(), sfu::error::Error> {
    let (ws_tx, ws_rx) = ws.split();
    let mut sync_channel = WebsocketSyncChannel {socket: ws_tx};
    let (tx, rx) = tokio::sync::oneshot::channel();
    server.send(ServerMessage::GetRoomAddr { room_id, response_channel: tx }).await?;
    let (room_name, room) = match rx.await? {
        Some(room) => room,
        None => {
            sync_channel.send("Room not found".into()).await;
            return Ok(());
        }
    };
    let message = serde_json::to_string(&SignalMessage::RoomInfo { name: room_name })?;
    sync_channel.send(message).await;
    let user = User::new(sync_channel, room.clone()).await?;
    let user_id = user.peer_id;
    let addr = user.start();
    addr.add_stream(ws_rx, |msg| {
        let message = match msg {
            Ok(Message::Text(text)) => {
                match serde_json::from_slice(text.as_bytes()) {
                    Ok(msg) => SyncMessage::Message(msg),
                    Err(e) => SyncMessage::Error(e.to_string()) 
                }
            },
            Ok(Message::Close(_) ) => return actor::StreamItem::Next(UserMessage::SyncMessage(SyncMessage::Close)) ,
            Err(e) => SyncMessage::Error(e.to_string()),
            _ => return actor::StreamItem::Skip
        };
        actor::StreamItem::Next(UserMessage::SyncMessage(message)) 
    });
    server.send(ServerMessage::JoinRoom { room_id, user_id, addr }).await?;
    Ok(())
}

pub struct WebsocketSyncChannel {
    socket: SplitSink<WebSocket, Message>
}

impl SyncChannel for WebsocketSyncChannel {
    type Item = String;
    type Error = axum::Error;
    async fn send(&mut self, message: String) -> Result<(), Self::Error> {
        self.socket.send(Message::Text(message.into()))
            .await?;
        Ok(())
    }
}

pub struct FileStorage {
    file: tokio::fs::File
}
pub struct FileConfiguration { path: String }

impl StorageConfiguration for FileConfiguration {
    type Error = std::env::VarError;
    fn from_env() -> Result<Self, Self::Error> {
        let output_dir = std::env::var("output_dir")?;
        let file_name = format!("{}.txt", Uuid::new_v4());
        let path = format!("{output_dir}/{file_name}");
        Ok(Self {
            path
        })
    }
}
 
impl Storage for FileStorage {
    type Configuration = FileConfiguration;
    type Error = std::io::Error;
    async fn connect(configuration: &Self::Configuration) -> Result<Self, Self::Error> {
        let file = File::create_new(&configuration.path).await?;
        Ok(Self {
            file
        })
    }
    async fn insert(&mut self, item: sfu::StorageItem<'_>) -> Result<(), Self::Error> {
        let raw = format!("[{} {}] {:?}", item.connection_id, item.timestamp, item.stats, );
        self.file.write_all(raw.as_bytes()).await?;
        Ok(())
    }
}