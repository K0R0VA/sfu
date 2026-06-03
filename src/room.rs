use std::{collections::HashMap};
use webrtc::{rtp::packet::Packet};

use crate::{actor::{Actor, Addr, Ctx}, user::{User, UserMessage}};

#[derive(Default)]
pub struct Room {
    peers: HashMap<String, Peer>
}

pub struct Peer {
    pub user: Addr<User>,
    pub stream: tokio::sync::broadcast::Receiver<Packet>,
    pub codec_mime_type: String,
}


pub enum RoomMessage {
    Join {
        peer_id: String,
        peer: Peer
    },
    // Выход пользователя
    Leave {
        peer_id: String,
    },
}

impl Actor for Room {
    type Message = RoomMessage;
    async fn handle(&mut self, _ctx: &Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            RoomMessage::Join { peer_id, peer } => {
                let Peer { user, stream, codec_mime_type } = peer;
                // old user receive new stream
                for (_, Peer { user, .. }) in self.peers.iter() {
                    let _ = user.send(UserMessage::ConnectToUser { 
                        speaker_id: peer_id.to_string(), 
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
            }
        }
    }   
    async fn starting(&mut self, _: &Ctx<'_, Self>) {
        println!("🟢 [RoomActor] Комната инициализирована.");
    }
    async fn stop(&mut self) {
        println!("🔴 [RoomActor] Комната уничтожена.");
    }
}