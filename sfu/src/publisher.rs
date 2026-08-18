use std::{str::FromStr, sync::Arc};
use rtc::rtp_transceiver::{ rtp_sender::RtpCodecKind};
use tokio::sync::mpsc::Receiver;
use uuid::Uuid;
use webrtc::{media_stream::track_remote::TrackRemote, peer_connection::{PeerConnection, PeerConnectionEventHandler, RTCIceCandidateInit, RTCPeerConnectionIceEvent, RTCSessionDescription}};
use crate::{SignalingClient, Storage, actor::{Actor, Addr, Ctx, StoppingExt}, audio_packet_forwarder::AudioPacketForwarder, create_peer, error::Error, keyframe_interceptor::{KeyframeInterceptor, RequestKeyframe}, quality_monitor::{QualityMonitor, QualityThresholds}, room::{AudioRouterStream, Codec, Room, RoomMessage, StreamQuality, VideoRouterStream}, rtp_packet_gateway_router::{AudioRouterContext, RtpPacketGatewayRouter, VideoRouterContext}, server::Key, simulcast_manager::SimulcastManager, user::{IceCandidate, MessageType, SignalMessage, Target, User, UserMessage}, video_packet_forwarder::VideoPacketForwarder};

pub struct Publisher<K: Key, C: SignalingClient<UserKey = K>, S: Storage> {
    pub peer_id: K,
    pub pc: Arc<dyn PeerConnection>,
    pub user: Addr<User<K, C, S>>,
    pub room: Addr<Room<K, C, S>>,    
    pub video_track_keyframe_interceptors: Vec<Addr<KeyframeInterceptor>>,
    pub quality_monitor: Addr<QualityMonitor<K, C, S>>,
    pub rx: Option<Receiver<PublisherMessage>>
}


const TARGET: Target = Target::Publisher;

impl<K: Key, C: SignalingClient<UserKey = K>, S: Storage> Publisher<K, C, S> {
    pub async fn new(user: Addr<User<K, C, S>>, room: Addr<Room<K, C, S>>, peer_id: K) -> Result<Self, Error> {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let handler =  PublisherPeerConnectionHandler { tx };
        let pc = create_peer(handler).await?;
        let pc: Arc<dyn PeerConnection> = pc.into();
        let thresholds = QualityThresholds::default();
        let quality_monitor = QualityMonitor::new(
            pc.clone(),
            user.clone(), 
            thresholds
        )
            .await?
            .start();
        Ok(Self {
            pc,
            user,
            room,
            peer_id,
            quality_monitor,
            video_track_keyframe_interceptors: Vec::with_capacity(3),
            rx: Some(rx)
        })
    }
    pub async fn try_stop(self) -> Result<(), Error> {
        self.pc.close().await?;
        Ok(())
    }
}

pub enum PublisherMessage {
    Websocket (MessageType),
    IceCandidate { 
        candidate: IceCandidate,
    },
    NewVideoTrack{ quality: StreamQuality, video_router_stream: VideoRouterStream },
    OnTrack(Arc<dyn TrackRemote>)
}

impl<K: Key, C: SignalingClient<UserKey = K>, S: Storage> Actor for Publisher<K, C, S> {
    type Message = PublisherMessage;
    async fn handle(&mut self, ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            PublisherMessage::NewVideoTrack {quality, video_router_stream } => {
                self.video_track_keyframe_interceptors.push(video_router_stream.keyframe_interceptor.clone());
                self.room.send(RoomMessage::AddVideoTrack { 
                    peer_id: self.peer_id, 
                    quality,
                    video_router_stream
                })
                .await
                .ok_or_terminate(ctx);
            },
            PublisherMessage::Websocket(message) => {
                self.handle_ws_message(message).await.ok_or_terminate(ctx);
            }
            PublisherMessage::IceCandidate { candidate } => {
               let message = SignalMessage::Rtc {
                    target: TARGET,
                    message_type: MessageType::Candidate { candidate }
                };
                self.user.send(UserMessage::SignalMessage(message)).await.ok_or_terminate(ctx);
            },
            PublisherMessage::OnTrack(track) => {
                let kind = track.kind().await;
                match kind {
                    RtpCodecKind::Audio | RtpCodecKind::Unspecified => { 
                        let ssrcs = track.ssrcs().await;
                        let ssrc = ssrcs[0];
                        let mime_type = track.codec(ssrc).await.unwrap().mime_type;
                        let codec = Codec::from_str(&mime_type).unwrap_or_default();
                        let router = RtpPacketGatewayRouter::<AudioPacketForwarder, AudioRouterContext>::spawn(track, AudioRouterContext {});
                        let _ = self.room.send(RoomMessage::AddAudioTrack { peer_id: self.peer_id, router: AudioRouterStream { router , codec } }).await;
                    },
                    RtpCodecKind::Video => {
                        SimulcastManager::spawn(track, ctx.addr.clone()).await.ok_or_terminate(ctx);
                    }   
                };
            }
        }
    }   
    async fn starting(&mut self, ctx: &Ctx<'_, Self>) {
        let addr = ctx.addr.clone();
        let mut rx = self.rx.take().expect("rx should pe created in Subscriber::new");
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let result = addr.send(message).await;
                if result.is_err() { break; }
            }
        });
    }
    async fn stopping(self, _: &Ctx<'_, Self>) {
        if let Err(e) = self.try_stop().await {
            tracing::error!("[Publisher] stopping {e}");
        }
        tracing::info!("🔴 [Publisher] Пользователь уничтожен.");
    }
}

impl<K: Key, C: SignalingClient<UserKey = K>, S: Storage> Publisher<K,C, S> {
    async fn handle_ws_message(&mut self, message: MessageType) -> Result<(), Error> {
        match message {
            MessageType::IceRestart { sdp } => {
                let offer_desc = RTCSessionDescription::offer(sdp)?;
                self.pc.set_remote_description(offer_desc).await?;
                let answer = self.pc.create_answer(None).await?;
                self.pc.set_local_description(answer.clone()).await?;
                let message = SignalMessage::Rtc {
                    target: Target::Publisher,
                    message_type: MessageType::Answer {sdp: answer.sdp }
                };
                self.user.send(UserMessage::SignalMessage(message)).await?;
            },
            MessageType::Offer { sdp } => {
                let offer_desc = RTCSessionDescription::offer(sdp)?;
                self.pc.set_remote_description(offer_desc).await?;
                let answer = self.pc.create_answer(None).await?;    
                self.pc.set_local_description(answer.clone()).await?;                  
                let message = SignalMessage::Rtc {                  
                    target: Target::Publisher,
                    message_type: MessageType::Answer {sdp: answer.sdp } 
                };
                self.user.send(UserMessage::SignalMessage(message)).await?;
                self.room.send(RoomMessage::SubscribeToPeers { peer_id: self.peer_id }).await?;
            },
            MessageType::Candidate { candidate } => {
                let IceCandidate { candidate, sdp_mid, sdp_mline_index } = candidate;
                let candidate_init = RTCIceCandidateInit {
                    candidate,
                    sdp_mid,
                    sdp_mline_index,
                    ..Default::default()
                };
                self.pc.add_ice_candidate(candidate_init).await?;
            },
            MessageType::Answer { sdp } => {
                let answer_desc = RTCSessionDescription::answer(sdp)?;
                self.pc.set_remote_description(answer_desc).await?;
            },
        }
        Ok(())
    }
}


pub struct PublisherPeerConnectionHandler {
    tx: tokio::sync::mpsc::Sender<PublisherMessage>
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for PublisherPeerConnectionHandler {
    async fn on_track(&self, track: Arc<dyn TrackRemote>) {
        let _ = self.tx.send(PublisherMessage::OnTrack(track)).await;
    }
    async fn on_ice_candidate(&self, event: RTCPeerConnectionIceEvent) {
        if let Ok(RTCIceCandidateInit { candidate, sdp_mid, sdp_mline_index, .. }) = event.candidate.to_json() {
            let _ = self.tx.send(PublisherMessage::IceCandidate { candidate: IceCandidate { candidate, sdp_mid, sdp_mline_index } }).await;
        }
    }
}