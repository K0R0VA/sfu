use std::{collections::HashMap, fmt::Display, str::FromStr};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Storage, SignalingClient, actor::{Actor, Addr, Ctx}, error::Error, keyframe_interceptor::KeyframeInterceptor, rtp_packet_gateway_router::{AudioRouter, RouterWaker, VideoRouter}, user::{ConnectionRequest, User, UserMessage}};

pub struct Room<C: SignalingClient, S: Storage> {
    pub id: Uuid,
    peers: HashMap<Uuid, Peer<C, S>>
}

impl<C: SignalingClient, S: Storage> Room<C, S> {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            peers: HashMap::new()
        }
    }
}

pub struct Peer<C: SignalingClient, S: Storage> {
    pub user: Addr<User<C, S>>,
    pub video_streams: HashMap<StreamQuality, VideoRouterStream>,
    pub audio_steam: Option<AudioRouterStream>
}

#[derive(Clone)]
pub struct AudioRouterStream {
    pub router: AudioRouter,
    pub codec: Codec
}

#[derive(Clone)]
pub struct VideoRouterStream {
    pub router: VideoRouter,
    pub codec: Codec,
    pub keyframe_interceptor: Addr<KeyframeInterceptor>,
    pub wake_tx: RouterWaker,
}

impl<C: SignalingClient, S: Storage> Peer<C, S> {
    fn add_audio_stream(&mut self, stream: AudioRouterStream) {
        self.audio_steam = Some(stream);
    }
    fn add_stream_stream(&mut self, quality: StreamQuality, stream: VideoRouterStream) {
        self.video_streams.insert(quality, stream);
    }
}

#[derive(Clone, Default)]
pub enum Codec {
    #[default]
    VP8,
    VP9,
    H264,
    AV1,
    Audio (String)
}

impl Into<u8> for Codec {
    fn into(self) -> u8 {
        match self {
            Codec::AV1 => 41,
            Codec::H264 => 102,
            Codec::VP8 => 96,
            Codec::VP9 => 100,
            Codec::Audio(_) => 0,
        }
    }
}

impl Display for Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Codec::H264 => write!(f, "video/H264"),
            Codec::VP8 => write!(f, "video/VP8"),
            Codec::VP9 => write!(f, "video/VP9"),
            Codec::AV1 => write!(f, "video/AV1"),
            Codec::Audio(audio) => write!(f, "{audio}")
        }
    }
}

impl FromStr for Codec {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "video/VP8" => Ok(Codec::VP8),
            "video/VP9" => Ok(Codec::VP9),
            "video/H264" => Ok(Codec::H264),
            "video/AV1" => Ok(Codec::AV1),
            mime_type => Ok(Codec::Audio(mime_type.to_string()))
        }
    }
}



#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd, Serialize, Deserialize, Default)]
#[repr(u8)]
pub enum StreamQuality {
    Audio = 0,
    Low = 1,
    Mid = 2,
    #[default]
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


pub enum RoomMessage<C: SignalingClient, S: Storage> {
    SubscribeToPeers { peer_id: Uuid },
    GetUser { peer_id: Uuid, response_channel: tokio::sync::oneshot::Sender<Option<Addr<User<C, S>>>>},
    Join {
        peer_id: Uuid,
        addr: Addr<User<C, S>>,
    },
    AddAudioTrack {
        peer_id: Uuid,
        router: AudioRouterStream
    },
    AddVideoTrack {
        peer_id: Uuid,
        quality: StreamQuality,
        video_router_stream: VideoRouterStream
    },
    Leave {
        peer_id: Uuid,
    },
}


impl<C: SignalingClient, S: Storage> Actor for Room<C, S> {
    type Message = RoomMessage<C, S>;
    async fn handle(&mut self, _ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            RoomMessage::Join { peer_id, addr } => {
                self.add_peer(peer_id, addr);
            }
            RoomMessage::GetUser { peer_id, response_channel } => {
                let user = self.peers.get(&peer_id).map(|p| p.user.clone());
                let _ = response_channel.send(user);
            }
            RoomMessage::SubscribeToPeers { peer_id } => {
                self.connect_old_streams(peer_id).await;
            }
            RoomMessage::AddAudioTrack { peer_id, router } => {
                self.connect_new_audio_stream(peer_id, router.clone()).await;
                let peer = self.peers.get_mut(&peer_id).unwrap();
                peer.add_audio_stream(router);
            },
            RoomMessage::AddVideoTrack { peer_id, quality, video_router_stream} => {
                self.connect_new_video_stream(peer_id, quality, video_router_stream.clone()).await;
                let peer = self.peers.get_mut(&peer_id).unwrap();
                peer.add_stream_stream(quality, video_router_stream);
            }
            RoomMessage::Leave { peer_id } => {
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

impl<C: SignalingClient, S: Storage> Room<C, S> {
    async fn connect_new_audio_stream(&mut self, peer_id: Uuid, stream: AudioRouterStream) {
        let peers = self.peers.iter()
            .filter(|(id, _)| **id != peer_id);
        for (_, peer) in peers {
            let Peer { user, .. } = peer;
            let _ = user.send(UserMessage::ConnectAudio(ConnectionRequest { 
                peer_id, 
                gateway_router: stream.router.clone(), 
                codec_mime_type: stream.codec.clone()
            })).await;
        }
    }
    async fn connect_new_video_stream(&mut self, peer_id: Uuid, quality: StreamQuality, video_router_stream: VideoRouterStream
    ) {
        let peers = self.peers.iter()
            .filter(|(id, _)| **id != peer_id);
        for (_, peer) in peers {
            let Peer { user, .. } = peer;
            let _ = user.send(UserMessage::ConnectVideo { 
                quality, 
                request: ConnectionRequest { 
                    peer_id, 
                    gateway_router: video_router_stream.router.clone(), 
                    codec_mime_type: video_router_stream.codec.clone(),
                },
                keyframe_interceptor: video_router_stream.keyframe_interceptor.clone(),
                wake_notification: video_router_stream.wake_tx.clone()
            }).await;
        }
    }
    async fn connect_old_streams(&mut self, peer_id: Uuid) {
        let Some(Peer { user, .. }) = self.peers.get(&peer_id) else { return ; };
        let stream = self.peers.iter()
            .filter(|(id, _)| **id != peer_id);
        for (existed_peer_id, Peer { audio_steam: audio_stream, video_streams, .. }) in stream {
            if let Some(audio_stream) = audio_stream {
                let _ = user.send(UserMessage::ConnectAudio(ConnectionRequest {
                    codec_mime_type: audio_stream.codec.clone(),
                    peer_id: *existed_peer_id,
                    gateway_router: audio_stream.router.clone()
                })).await;
            }
            for (quality, stream) in video_streams {
                let _ = user.send(UserMessage::ConnectVideo {
                    quality: *quality,
                    request: ConnectionRequest {
                        codec_mime_type: stream.codec.clone(),
                        peer_id: *existed_peer_id,
                        gateway_router: stream.router.clone()
                    },
                    keyframe_interceptor: stream.keyframe_interceptor.clone(),
                    wake_notification: stream.wake_tx.clone()
                }).await;
            }
        }
    }
    fn add_peer(&mut self, peer_id: Uuid, user: Addr<User<C, S>>) {
        if self.peers.contains_key(&peer_id) { return; }
        self.peers.insert(peer_id, Peer { user, video_streams: HashMap::with_capacity(3), audio_steam: None });
    }
}