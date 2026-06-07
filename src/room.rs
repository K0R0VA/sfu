use std::{collections::{HashMap, HashSet}, str::FromStr};
use uuid::Uuid;

use crate::{PacketSubscription, actor::{Actor, Addr, Ctx}, error::Error, user::{ConnectionRequest, ConnectionRequestKind, User, UserMessage}};

#[derive(Default)]
pub struct Room {
    peers: HashMap<Uuid, Peer>
}

pub struct Peer {
    pub user: Addr<User>,
    pub video_streams: HashMap<StreamQuality, PeerStream>,
    pub audio_stream: Option<PeerStream>
}

impl Peer {
    fn add_audio_track(&mut self, stream: PeerStream) {
        self.audio_stream = Some(stream);
    }
    fn add_stream_track(&mut self, quality: StreamQuality, stream: PeerStream) {
        self.video_streams.insert(quality, stream);
    }
}

pub struct PeerStream {
    pub packet_subscription: PacketSubscription,
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
    Join {
        peer_id: Uuid,
        addr: Addr<User>,
    },
    AddAudioTrack {
        peer_id: Uuid,
        stream: PeerStream
    },
    AddVideoTrack {
        peer_id: Uuid,
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
            RoomMessage::Join { peer_id, addr } => {
                self.add_peer(peer_id, addr);
            }
            RoomMessage::AddAudioTrack { peer_id, mut stream } => {
                self.connect_new_stream(peer_id, ConnectionRequestKind::Audio, &mut stream).await;
                self.connect_old_streams(peer_id).await;
                let peer = self.peers.get_mut(&peer_id).unwrap();
                peer.add_audio_track(stream);
            },
            RoomMessage::AddVideoTrack { peer_id, mut stream, quality } => {
                self.connect_new_stream(peer_id, ConnectionRequestKind::Video { stream_quality: quality }, &mut stream).await;
                self.connect_old_streams(peer_id).await;
                let peer = self.peers.get_mut(&peer_id).unwrap();
                peer.add_stream_track(quality, stream);
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
    async fn connect_new_stream(&mut self, peer_id: Uuid, kind: ConnectionRequestKind, stream: &mut PeerStream) {
        let peers = self.peers.iter_mut()
            .filter(|(id, _)| **id != peer_id);
        for (existed_peer_id, peer) in peers {
            let Peer { user, .. } = peer;
            stream.subscribers.insert(*existed_peer_id);
            let _ = user.send(UserMessage::ConnectToUser(ConnectionRequest {
                codec_mime_type: stream.mime_type.clone(),
                kind,
                speaker_id: peer_id,
                stream: stream.packet_subscription.clone()
            })).await;
        }
    }
    async fn connect_old_streams(&mut self, peer_id: Uuid) {
        let Some(Peer { user, .. }) = self.peers.get(&peer_id) else { return ; };
        let user = user.clone();
        let stream = self.peers.iter_mut()
            .filter(|(id, _)| **id != peer_id);
        for (existed_peer_id, Peer { audio_stream, video_streams, .. }) in stream {
            if let Some(audio_stream) = audio_stream {
                if !audio_stream.subscribers.contains(&peer_id) {
                    let _ = user.send(UserMessage::ConnectToUser(ConnectionRequest {
                        codec_mime_type: audio_stream.mime_type.clone(),
                        kind: ConnectionRequestKind::Audio,
                        speaker_id: *existed_peer_id,
                        stream: audio_stream.packet_subscription.clone()
                    })).await;
                    audio_stream.subscribers.insert(peer_id);
                }
            }
            for (quaity, video_stream) in video_streams {
                if !video_stream.subscribers.contains(&peer_id) {
                    let _ = user.send(UserMessage::ConnectToUser(ConnectionRequest {
                        codec_mime_type: video_stream.mime_type.clone(),
                        kind: ConnectionRequestKind::Video { stream_quality: *quaity },
                        speaker_id: *existed_peer_id,
                        stream: video_stream.packet_subscription.clone()
                    })).await;
                    video_stream.subscribers.insert(peer_id);
                }
            }
        }
    }
    fn add_peer(&mut self, peer_id: Uuid, user: Addr<User>) {
        self.peers.insert(peer_id, Peer { user, video_streams: HashMap::with_capacity(3), audio_stream: None });
    }
}