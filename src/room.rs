use std::{collections::HashMap, sync::Arc};
use webrtc::{rtp::packet::Packet, track::track_local::track_local_static_rtp::TrackLocalStaticRTP};

use crate::{actor::{Actor, Addr, Ctx}, user::{User, UserMessage}};

#[derive(Default)]
pub struct Room {
    peers: HashMap<String, Peer>
}

pub struct Peer {
    pub user: Addr<User>,
    pub track: Arc<TrackLocalStaticRTP>
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
    Broadcast {
        stream: tokio::sync::broadcast::Receiver<Packet>,
        peer_id: String
    }
}

impl Actor for Room {
    type Message = RoomMessage;
    async fn handle(&mut self, _ctx: &Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            RoomMessage::Join { peer_id, peer } => {
                let Peer { user, track } = peer;
                for (_, Peer { user: existed_user, .. }) in self.peers.iter() {
                    let _ = existed_user.send(UserMessage::ConnectToUser { 
                        speaker_id: peer_id.to_string(), 
                        track: track.clone()
                    }).await;
                }
                for (existing_id, Peer { track, .. }) in self.peers.iter() {
                    let _ = user.send(UserMessage::ConnectToUser { 
                        speaker_id: existing_id.clone(), 
                        track: track.clone()
                    }).await;
                }
                println!("👤 [RoomActor] Участник {} зашел в комнату", peer_id);
                self.peers.insert(peer_id, Peer { user, track });
            }
            RoomMessage::Leave { peer_id } => {
                println!("❌ [RoomActor] Участник {} вышел из комнаты", peer_id);
                self.peers.remove(&peer_id);
            }
            RoomMessage::Broadcast { stream, peer_id } => {
                for (_, Peer { user, .. }) in self.peers.iter().filter(|(key, _)| key != &&peer_id) {
                    let _ = user.send(UserMessage::Broadcast { stream: stream.resubscribe(), speaker_id: peer_id.clone() }).await;
                }
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