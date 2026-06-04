use std::{collections::HashMap};
use uuid::Uuid;
use webrtc::{rtp::packet::Packet};

use crate::{actor::{Actor, Addr, Ctx}, user::{User, UserMessage}};

#[derive(Default)]
pub struct Room {
    peers: HashMap<Uuid, Peer>
}

pub struct Peer {
    pub user: Addr<User>,
    pub stream: tokio::sync::broadcast::Receiver<Packet>,
    pub codec_mime_type: String,
}


pub enum RoomMessage {
    Join {
        peer_id: Uuid,
        peer: Peer
    },
    // Выход пользователя
    Leave {
        peer_id: Uuid,
    },
}

impl Actor for Room {
    type Message = RoomMessage;
    async fn handle(&mut self, _ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            RoomMessage::Join { peer_id, peer } => {
                let Peer { user, stream, codec_mime_type } = peer;
                // old user receive new stream
                for (_, Peer { user, .. }) in self.peers.iter() {
                    let _ = user.send(UserMessage::ConnectToUser { 
                        speaker_id: peer_id, 
                        stream: stream.resubscribe(),
                        codec_mime_type: codec_mime_type.clone()
                    }).await;
                }
                // new user receive old streams
                for (peer_id, Peer { stream, codec_mime_type, .. }) in self.peers.iter() {
                    let _ = user.send(UserMessage::ConnectToUser { 
                        speaker_id: peer_id.clone(), 
                        stream: stream.resubscribe(),
                        codec_mime_type: codec_mime_type.clone()
                    }).await;
                }
                println!("👤 [RoomActor] Участник {} зашел в комнату", peer_id);
                self.peers.insert(peer_id, Peer { user, stream, codec_mime_type });
            }
            RoomMessage::Leave { peer_id } => {
                println!("❌ [RoomActor] Участник {} вышел из комнаты", peer_id);
                self.peers.remove(&peer_id);
                for (_, Peer { user, .. }) in self.peers.iter() {
                    let _ = user.send(UserMessage::DisconnectFromUser { speaker_id: peer_id }).await;
                }
            }
        }
    }   
    async fn starting(&mut self, _: &Ctx<'_, Self>) {
        println!("🟢 [RoomActor] Комната инициализирована.");
    }
    async fn stopping(&mut self, _ctx: &Ctx<'_, Self>) {
        for (_, Peer { user, .. }) in self.peers.iter() {
            let _ = user.send(UserMessage::RoomClosed).await;
        }
        println!("🔴 [RoomActor] Комната уничтожена.");
    }
}