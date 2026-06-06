use std::{collections::{HashMap, HashSet}, str::FromStr};
use uuid::Uuid;

use crate::{PacketStream, actor::{Actor, Addr, Ctx}, error::Error, user::{ConnectionRequest, ConnectionRequestKind, User, UserMessage}};

#[derive(Default)]
pub struct Room {
    peers: HashMap<Uuid, Peer>
}

pub struct Peer {
    pub user: Addr<User>,
    pub video_streams: HashMap<StreamQuality, PeerStream>,
    pub audio_stream: Option<PeerStream>
}

pub struct PeerStream {
    pub stream: PacketStream,
    pub mime_type: String,
    pub subscribers: HashSet<Uuid>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum StreamQuality {
    Low,
    Mid,
    High,
}

impl FromStr for StreamQuality {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let rid = match s {
            "low" => StreamQuality::Low,
            "mid" => StreamQuality::Mid,
            "high" => StreamQuality::High,
            _ => return Err(Error::SystemError { message: "Unknown StreamQuality type".into() })
        };
        Ok(rid)
    }
}


pub enum RoomMessage {
    JoinAudio {
        peer_id: Uuid,
        addr: Addr<User>,
        stream: PeerStream
    },
    JoinVideo {
        peer_id: Uuid,
        addr: Addr<User>,
        stream: PeerStream,
        quality: StreamQuality
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
            RoomMessage::JoinAudio { peer_id, addr, mut stream } => {
                self.connect_old_peers_to_new_stream(peer_id, ConnectionRequestKind::Audio, &mut stream).await;
                self.connect_new_peer_to_streams(peer_id, &addr).await;
                self.add_peer_audio_track(peer_id, addr, stream).await;
            },
            RoomMessage::JoinVideo { peer_id, addr, mut stream, quality } => {
                self.connect_old_peers_to_new_stream(peer_id, ConnectionRequestKind::Video { stream_quality: quality }, &mut stream).await;
                self.connect_new_peer_to_streams(peer_id, &addr).await;
                self.add_peer_video_track(peer_id, addr, stream, quality).await;
            }
            RoomMessage::Leave { peer_id } => {
                tracing::info!("❌ [RoomActor] Участник {} вышел из комнаты", peer_id);
                self.peers.remove(&peer_id);
                for (_, Peer { user, .. }) in self.peers.iter() {
                    let _ = user.send(UserMessage::DisconnectFromUser { speaker_id: peer_id }).await;
                }
            }
        }
    }   
    async fn starting(&mut self, _: &Ctx<'_, Self>) {
        tracing::info!("🟢 [RoomActor] Комната инициализирована.");
    }
    async fn stopping(&mut self, _ctx: &Ctx<'_, Self>) {
        for (_, Peer { user, .. }) in self.peers.iter() {
            let _ = user.send(UserMessage::RoomClosed).await;
        }
        tracing::info!("🔴 [RoomActor] Комната уничтожена.");
    }
}

impl Room {
    async fn connect_old_peers_to_new_stream(&self, peer_id: Uuid, kind: ConnectionRequestKind, stream: &mut PeerStream) {
        for (existed_peer_id, Peer { user, .. }) in self.peers.iter().filter(|(id, _)| **id != peer_id) {
            tracing::info!("👤 [RoomActor] Отправляется ConnectToUser({:?}) {existed_peer_id} to {peer_id}", kind);
            stream.subscribers.insert(*existed_peer_id);
            let _ = user.send(UserMessage::ConnectToUser(ConnectionRequest {
                codec_mime_type: stream.mime_type.clone(),
                kind,
                speaker_id: peer_id,
                stream: stream.stream.resubscribe()
            })).await;
        }
    }
    async fn connect_new_peer_to_streams(&mut self, peer_id: Uuid, addr: &Addr<User>) {
        let stream = self.peers.iter_mut()
            .filter(|(id, _)| **id != peer_id);
        for (existed_peer_id, Peer { audio_stream, video_streams, .. }) in stream {
            if let Some(audio_stream) = audio_stream {
                if !audio_stream.subscribers.contains(&peer_id) {
                    tracing::info!("👤 [RoomActor] Отправляется ConnectToUser(audio) {peer_id} to {existed_peer_id}");
                    let _ = addr.send(UserMessage::ConnectToUser(ConnectionRequest {
                        codec_mime_type: audio_stream.mime_type.clone(),
                        kind: ConnectionRequestKind::Audio,
                        speaker_id: *existed_peer_id,
                        stream: audio_stream.stream.resubscribe()
                    })).await;
                    audio_stream.subscribers.insert(peer_id);
                }
            }
            for (quaity, video_stream) in video_streams {
                if !video_stream.subscribers.contains(&peer_id) {
                    tracing::info!("👤 [RoomActor] Отправляется ConnectToUser(video) {peer_id} to {existed_peer_id}");
                    let _ = addr.send(UserMessage::ConnectToUser(ConnectionRequest {
                        codec_mime_type: video_stream.mime_type.clone(),
                        kind: ConnectionRequestKind::Video { stream_quality: *quaity },
                        speaker_id: *existed_peer_id,
                        stream: video_stream.stream.resubscribe()
                    })).await;
                    video_stream.subscribers.insert(peer_id);
                }
            }
        }
    }
    async fn add_peer_audio_track(&mut self, peer_id: Uuid, addr: Addr<User>, stream: PeerStream) {
        match self.peers.entry(peer_id) {
            std::collections::hash_map::Entry::Occupied(mut peer) => {
                let peer = peer.get_mut();
                peer.audio_stream = Some(stream);
                tracing::info!("👤 [RoomActor] Участник {} поключил audio", peer_id);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let audio_stream = Some(stream);
                let video_streams = HashMap::with_capacity(3);
                let peer = Peer {audio_stream, user: addr, video_streams};
                entry.insert(peer);
                tracing::info!("👤 [RoomActor] Участник {} зашел в комнату", peer_id);
            }
        }
    }
    async fn add_peer_video_track(&mut self, peer_id: Uuid, addr: Addr<User>, stream: PeerStream, quality: StreamQuality) {
        match self.peers.entry(peer_id) {
            std::collections::hash_map::Entry::Occupied(mut peer) => {
                let peer = peer.get_mut();
                peer.video_streams.insert(quality, stream);
                tracing::info!("👤 [RoomActor] Участник {} поключил audio", peer_id);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let audio_stream = None;
                let mut video_streams = HashMap::with_capacity(3);
                video_streams.insert(quality, stream);
                let peer = Peer {audio_stream, user: addr, video_streams};
                entry.insert(peer);
                tracing::info!("👤 [RoomActor] Участник {} зашел в комнату", peer_id);
            }
        }
    }
}