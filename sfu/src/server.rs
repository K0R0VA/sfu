use std::{collections::HashMap, hash::Hash};
use serde::Serialize;
use uuid::Uuid;

use crate::{Storage, SignalingClient, actor::{Actor, Addr, Ctx}, error::Error, room::Room};

pub struct Server<K, C: SignalingClient, S: Storage> {
    rooms: HashMap<K, Addr<Room<C, S>>>
}

#[derive(Serialize)]
pub struct RoomResponse {
    id: Uuid,
}

impl<K: RoomKey, C: SignalingClient, S: Storage> Addr<Server<K, C, S>> {
    pub async fn create_room(&self, id: K) -> Result<Addr<Room<C, S>>, Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.send(ServerMessage::CreateRoom { id, response_channel: tx }).await;
        Ok(rx.await?)
    }
    pub async fn get_room(&self, room_id: K) -> Result<Addr<Room<C, S>>, Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.send(ServerMessage::GetRoomAddr { room_id, response_channel: tx } ).await;
        let room = rx.await?.ok_or(Error::SystemError { message: "Room not found".into() })?;
        Ok(room)
    }
}

impl<K: RoomKey, C: SignalingClient, S: Storage> Default for Server<K, C, S> {
    fn default() -> Self {
        Self {
            rooms: HashMap::new()
        }
    }
}



pub enum ServerMessage<K: RoomKey, C: SignalingClient, S: Storage> {
    CreateRoom { id: K, response_channel: tokio::sync::oneshot::Sender<Addr<Room<C, S>>> },
    DeleteRoom { room_id: K },
    GetRoomAddr { room_id: K, response_channel: tokio::sync::oneshot::Sender<Option<Addr<Room<C, S>>>> },
}

pub trait RoomKey: 'static + Send + Hash + Eq {
    
}


impl<K: RoomKey, C: SignalingClient, S: Storage> Actor for Server<K, C, S> {
    type Message = ServerMessage<K, C, S>;
    async fn handle(&mut self, _ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            ServerMessage::CreateRoom { id, response_channel } => {
                let new_room = Room::new();
                let room_addr = new_room.start();
                self.rooms.insert(id, room_addr.clone());
                let _ = response_channel.send(room_addr);
            },
            ServerMessage::DeleteRoom { room_id } => {
                if let Some(room) = self.rooms.remove(&room_id) {
                    room.terminate().await.ok();
                }
            }
            ServerMessage::GetRoomAddr { room_id, response_channel } => {
                let room = self.rooms.get(&room_id).cloned();
                let _ = response_channel.send(room);
            }
        }
    }   
    async fn starting(&mut self, _: &Ctx<'_, Self>) {
        tracing::info!("🟢 [RoomActor] Комната инициализирована.");
    }
    async fn stopping(self, _ctx: &Ctx<'_, Self>) {
        for room in self.rooms.into_values() {
            room.terminate().await.ok();
        }
        tracing::info!("🔴 [RoomActor] Комната уничтожена.");
    }
}

impl<K: Eq + Hash + Send + 'static> RoomKey for K {} 