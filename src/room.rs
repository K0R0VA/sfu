use std::{collections::{HashMap}, str::FromStr};
use uuid::Uuid;

use crate::{PacketAudioSubscription, PacketVideoSubscription, actor::{Actor, Addr, Ctx}, error::Error, pli_sender::Ping, user::{ConnectionRequest, User, UserMessage}};

#[derive(Default)]
pub struct Room {
    peers: HashMap<Uuid, Peer>
}

pub struct Peer {
    pub user: Addr<User>,
    pub video_streams: HashMap<StreamQuality, PeerStream<PacketVideoSubscription>>,
    pub audio_stream: Option<PeerStream<PacketAudioSubscription>>
}

impl Peer {
    fn add_audio_track(&mut self, stream: PeerStream<PacketAudioSubscription>) {
        self.audio_stream = Some(stream);
    }
    fn add_stream_track(&mut self, quality: StreamQuality, stream: PeerStream<PacketVideoSubscription>) {
        self.video_streams.insert(quality, stream);
    }
}

pub struct PeerStream<T> {
    pub packet_subscription: T,
    pub mime_type: String,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd)]
#[repr(u8)]
pub enum StreamQuality {
    Low = 0,
    Mid = 1,
    High = 2,
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
    SubscribeToPeers { peer_id: Uuid },
    Join {
        peer_id: Uuid,
        addr: Addr<User>,
    },
    AddAudioTrack {
        peer_id: Uuid,
        stream: PeerStream<PacketAudioSubscription>
    },
    AddVideoTrack {
        peer_id: Uuid,
        stream: PeerStream<PacketVideoSubscription>,
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
            RoomMessage::SubscribeToPeers { peer_id } => {
                self.connect_old_streams(peer_id).await;
            }
            RoomMessage::AddAudioTrack { peer_id, mut stream } => {
                self.connect_new_audio_stream(peer_id, &mut stream).await;
                let peer = self.peers.get_mut(&peer_id).unwrap();
                peer.add_audio_track(stream);
            },
            RoomMessage::AddVideoTrack { peer_id, mut stream, quality } => {
                self.connect_new_video_stream(peer_id, quality, &mut stream).await;
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
    async fn connect_new_audio_stream(&mut self, peer_id: Uuid, stream: &mut PeerStream<PacketAudioSubscription>) {
        let peers = self.peers.iter()
            .filter(|(id, _)| **id != peer_id);
        for (_, peer) in peers {
            let Peer { user, .. } = peer;
            let _ = user.send(UserMessage::ConnectAudio(ConnectionRequest { 
                speaker_id: peer_id, 
                stream: stream.packet_subscription.clone(), 
                codec_mime_type: stream.mime_type.clone()
            })).await;
        }
    }
    async fn connect_new_video_stream(&mut self, peer_id: Uuid, quality: StreamQuality, stream: &mut PeerStream<PacketVideoSubscription>) {
        let peers = self.peers.iter()
            .filter(|(id, _)| **id != peer_id);
        for (_, peer) in peers {
            let Peer { user, .. } = peer;
            let _ = user.send(UserMessage::ConnectVideo{ 
                quality, 
                request: ConnectionRequest { 
                    speaker_id: peer_id, 
                    stream: stream.packet_subscription.clone(), 
                    codec_mime_type: stream.mime_type.clone()
                }
            }).await;
        }
    }
    async fn connect_old_streams(&mut self, peer_id: Uuid) {
        let Some(Peer { user, .. }) = self.peers.get(&peer_id) else { return ; };
        let stream = self.peers.iter()
            .filter(|(id, _)| **id != peer_id);
        for (existed_peer_id, Peer { audio_stream, video_streams, .. }) in stream {
            if let Some(audio_stream) = audio_stream {
                let _ = user.send(UserMessage::ConnectAudio(ConnectionRequest {
                    codec_mime_type: audio_stream.mime_type.clone(),
                    speaker_id: *existed_peer_id,
                    stream: audio_stream.packet_subscription.clone()
                })).await;
            }
            for (quaity, video_stream) in video_streams {
                let _ = video_stream.packet_subscription.pli_sender.send(Ping).await;
                let _ = user.send(UserMessage::ConnectVideo {
                    quality: *quaity,
                    request: ConnectionRequest {
                        codec_mime_type: video_stream.mime_type.clone(),
                        speaker_id: *existed_peer_id,
                        stream: video_stream.packet_subscription.clone()
                    }
                }).await;
            }
        }
    }
    fn add_peer(&mut self, peer_id: Uuid, user: Addr<User>) {
        self.peers.insert(peer_id, Peer { user, video_streams: HashMap::with_capacity(3), audio_stream: None });
    }
}