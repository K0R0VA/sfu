use std::{collections::HashMap, fmt::{Display}, str::FromStr};
use uuid::Uuid;
use webrtc::rtp::packet::Packet;

use crate::{Storage, SyncChannel, actor::{Actor, Addr, Ctx}, audio_packet_forwarder::AudioPacketForwarder, error::Error, keyframe_interceptor::KeyframeInterceptor, rtp_packet_gateway_router::RtpPacketGatewayRouter, server::Server, user::{ConnectionRequest, User, UserMessage}, video_packet_forwarder::VideoPacketForwarder};

pub struct Room<C: SyncChannel, S: Storage> {
    pub id: Uuid,
    peers: HashMap<Uuid, Peer<C, S>>
}

impl<C: SyncChannel, S: Storage> Room<C, S> {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            peers: HashMap::new()
        }
    }
}

pub struct Peer<C: SyncChannel, S: Storage> {
    pub user: Addr<User<C, S>>,
    pub video_tracks: HashMap<StreamQuality, (PeerTrack<VideoPacketForwarder>, Addr<KeyframeInterceptor>)>,
    pub audio_track: Option<PeerTrack<AudioPacketForwarder>>
}

impl<C: SyncChannel, S: Storage> Peer<C, S> {
    fn add_audio_track(&mut self, stream: PeerTrack<AudioPacketForwarder>) {
        self.audio_track = Some(stream);
    }
    fn add_stream_track(&mut self, quality: StreamQuality, stream: PeerTrack<VideoPacketForwarder>, keyframe_interceptor: Addr<KeyframeInterceptor>) {
        self.video_tracks.insert(quality, (stream, keyframe_interceptor));
    }
}

pub struct PeerTrack<T: Actor> where T::Message: From<(StreamQuality, Packet)> {
    pub gateway_router:  Addr<RtpPacketGatewayRouter<T>>,
    pub mime_type: MimeType,
}

#[derive(Clone, Default)]
pub enum MimeType {
    #[default]
    VP8,
    VP9,
    H264,
    Audio (String)
}

impl Display for MimeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MimeType::H264 => write!(f, "video/H264"),
            MimeType::VP8 => write!(f, "video/VP8"),
            MimeType::VP9 => write!(f, "video/VP9"),
            MimeType::Audio(audio) => write!(f, "{audio}")

        }
    }
}

impl FromStr for MimeType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "video/VP8" => Ok(MimeType::VP8),
            "video/VP9" => Ok(MimeType::VP9),
            "video/H264" => Ok(MimeType::H264),
            mime_type => Ok(MimeType::Audio(mime_type.to_string()))
        }
    }
}



#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd)]
#[repr(u8)]
pub enum StreamQuality {
    Audio = 0,
    Low = 1,
    Mid = 2,
    High = 3,
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


pub enum RoomMessage<C: SyncChannel, S: Storage> {
    SubscribeToPeers { peer_id: Uuid },
    Join {
        peer_id: Uuid,
        addr: Addr<User<C, S>>,
    },
    AddAudioTrack {
        peer_id: Uuid,
        track: PeerTrack<AudioPacketForwarder>
    },
    AddVideoTrack {
        peer_id: Uuid,
        track: PeerTrack<VideoPacketForwarder>,
        keyframe_interceptor: Addr<KeyframeInterceptor>,
        quality: StreamQuality
    },
    // Выход пользователя
    Leave {
        peer_id: Uuid,
    },
}


impl<C: SyncChannel, S: Storage> Actor for Room<C, S> {
    type Message = RoomMessage<C, S>;
    async fn handle(&mut self, _ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            RoomMessage::Join { peer_id, addr } => {
                self.add_peer(peer_id, addr);
            }
            RoomMessage::SubscribeToPeers { peer_id } => {
                self.connect_old_streams(peer_id).await;
            }
            RoomMessage::AddAudioTrack { peer_id, track: mut stream } => {
                self.connect_new_audio_stream(peer_id, &mut stream).await;
                let peer = self.peers.get_mut(&peer_id).unwrap();
                peer.add_audio_track(stream);
            },
            RoomMessage::AddVideoTrack { peer_id, track: mut stream, keyframe_interceptor, quality } => {
                tracing::info!("[RoomActor] AddVideoTrack");
                self.connect_new_video_stream(peer_id, quality, &mut stream, keyframe_interceptor.clone()).await;
                let peer = self.peers.get_mut(&peer_id).unwrap();
                peer.add_stream_track(quality, stream, keyframe_interceptor);
            }
            RoomMessage::Leave { peer_id } => {
                tracing::info!("❌ [RoomActor] Участник {} вышел из комнаты", peer_id);
                self.peers.remove(&peer_id);
                for (_, Peer { user, .. }) in self.peers.iter() {
                    let _ = user.send(UserMessage::Unsubscribe { user_id: peer_id }).await;
                }
            }
        }
    }   
    async fn starting(&mut self, _: &Ctx<'_, Self>) {
        tracing::info!("🟢 [RoomActor] Комната инициализирована.");
    }
    async fn stopping(self, _ctx: &Ctx<'_, Self>) {
        for (_, Peer { user, .. }) in self.peers.iter() {
            let _ = user.send(UserMessage::RoomClosed).await;
        }
        tracing::info!("🔴 [RoomActor] Комната уничтожена.");
    }
}

impl<C: SyncChannel, S: Storage> Room<C, S> {
    async fn connect_new_audio_stream(&mut self, peer_id: Uuid, stream: &mut PeerTrack<AudioPacketForwarder>) {
        let peers = self.peers.iter()
            .filter(|(id, _)| **id != peer_id);
        for (_, peer) in peers {
            let Peer { user, .. } = peer;
            let _ = user.send(UserMessage::ConnectAudio(ConnectionRequest { 
                peer_id, 
                gateway_router: stream.gateway_router.clone(), 
                codec_mime_type: stream.mime_type.clone()
            })).await;
        }
    }
    async fn connect_new_video_stream(&mut self, peer_id: Uuid, quality: StreamQuality, stream: &mut PeerTrack<VideoPacketForwarder>, keyframe_interceptor: Addr<KeyframeInterceptor>) {
        let peers = self.peers.iter()
            .filter(|(id, _)| **id != peer_id);
        for (_, peer) in peers {
            let Peer { user, .. } = peer;
            let _ = user.send(UserMessage::ConnectVideo { 
                quality, 
                request: ConnectionRequest { 
                    peer_id, 
                    gateway_router: stream.gateway_router.clone(), 
                    codec_mime_type: stream.mime_type.clone(),
                },
                keyframe_interceptor: keyframe_interceptor.clone()
            }).await;
        }
    }
    async fn connect_old_streams(&mut self, peer_id: Uuid) {
        let Some(Peer { user, .. }) = self.peers.get(&peer_id) else { return ; };
        let stream = self.peers.iter()
            .filter(|(id, _)| **id != peer_id);
        for (existed_peer_id, Peer { audio_track: audio_stream, video_tracks: video_streams, .. }) in stream {
            if let Some(audio_stream) = audio_stream {
                let _ = user.send(UserMessage::ConnectAudio(ConnectionRequest {
                    codec_mime_type: audio_stream.mime_type.clone(),
                    peer_id: *existed_peer_id,
                    gateway_router: audio_stream.gateway_router.clone()
                })).await;
            }
            for (quality, (video_stream, keyframe_interceptor)) in video_streams {
                let _ = user.send(UserMessage::ConnectVideo {
                    quality: *quality,
                    keyframe_interceptor: keyframe_interceptor.clone(),
                    request: ConnectionRequest {
                        codec_mime_type: video_stream.mime_type.clone(),
                        peer_id: *existed_peer_id,
                        gateway_router: video_stream.gateway_router.clone()
                    }
                }).await;
            }
        }
    }
    fn add_peer(&mut self, peer_id: Uuid, user: Addr<User<C, S>>) {
        self.peers.insert(peer_id, Peer { user, video_tracks: HashMap::with_capacity(3), audio_track: None });
    }
}