use std::{collections::HashMap};
use serde::Serialize;
use uuid::Uuid;

use crate::{Storage, SyncChannel, actor::{Actor, Addr, Ctx}, error::Error, room::{Room, RoomMessage}, user::User};

pub struct Server<C: SyncChannel, S: Storage> {
    rooms: HashMap<Uuid, (String, Addr<Room<C, S>>)>
}

#[derive(Serialize)]
pub struct RoomResponse {
    id: Uuid,
    name: String
}

impl<C: SyncChannel, S: Storage> Addr<Server<C, S>> {
    pub async fn create_room(&self, name: String) -> Result<(Uuid, Addr<Room<C, S>>), Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.send(ServerMessage::CreateRoom { name, response_channel: tx }).await;
        Ok(rx.await?)
    }
    pub async fn get_room(&self, room_id: Uuid) -> Result<(String, Addr<Room<C, S>>), Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.send(ServerMessage::GetRoomAddr { room_id, response_channel: tx } ).await;
        let room = rx.await?.ok_or(Error::SystemError { message: "Room not found".into() })?;
        Ok(room)
    }
    pub async fn get_rooms(&self) -> Result<Vec<RoomResponse>, Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.send(ServerMessage::GetRooms { response_channel: tx } ).await;
        let rooms = rx.await?;
        Ok(rooms)
    }
}

impl<C: SyncChannel, S: Storage> Default for Server<C, S> {
    fn default() -> Self {
        Self {
            rooms: HashMap::new()
        }
    }
}



pub enum ServerMessage<C: SyncChannel, S: Storage> {
    CreateRoom { name: String, response_channel: tokio::sync::oneshot::Sender<(Uuid, Addr<Room<C, S>>)> },
    DeleteRoom { room_id: Uuid },
    GetRoomAddr { room_id: Uuid, response_channel: tokio::sync::oneshot::Sender<Option<(String, Addr<Room<C, S>>)>> },
    GetRooms { response_channel: tokio::sync::oneshot::Sender<Vec<RoomResponse>> },
    JoinRoom { room_id: Uuid, user_id: Uuid, addr: Addr<User<C, S>> },
}


impl<C: SyncChannel, S: Storage> Actor for Server<C, S> {
    type Message = ServerMessage<C, S>;
    async fn handle(&mut self, ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            ServerMessage::CreateRoom { name, response_channel } => {
                let new_room = Room::new();
                let room_id = new_room.id;
                let room_addr = new_room.start();
                self.rooms.insert(room_id, (name, room_addr.clone()));
                let _ = response_channel.send((room_id, room_addr));
            },
            ServerMessage::DeleteRoom { room_id } => {
                if let Some((_, room)) = self.rooms.remove(&room_id) {
                    room.terminate().await.ok();
                }
            }
            ServerMessage::GetRoomAddr { room_id, response_channel } => {
                let room = self.rooms.get(&room_id).cloned();
                let _ = response_channel.send(room);
            }
            ServerMessage::GetRooms { response_channel } => {
                let rooms = self.rooms.iter().map(|(id, (name, _))| RoomResponse { id: *id, name: name.clone() }).collect();
                let _ = response_channel.send(rooms);
            }
            ServerMessage::JoinRoom { room_id, user_id, addr } => {
                if let Some((_, room)) = self.rooms.get(&room_id) {
                    let _ = room.send(RoomMessage::Join { peer_id: user_id, addr }).await;
                }
            }
        }
    }   
    async fn starting(&mut self, _: &Ctx<'_, Self>) {
        tracing::info!("🟢 [RoomActor] Комната инициализирована.");
    }
    async fn stopping(self, _ctx: &Ctx<'_, Self>) {
        for (_, room) in self.rooms.into_values() {
            room.terminate().await.ok();
        }
        tracing::info!("🔴 [RoomActor] Комната уничтожена.");
    }
}