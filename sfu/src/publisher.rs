use std::{str::FromStr, sync::Arc};
use tokio::sync::Notify;
use uuid::Uuid;
use webrtc::{ice_transport::{ice_candidate::RTCIceCandidateInit, ice_connection_state::RTCIceConnectionState}, peer_connection::{RTCPeerConnection, sdp::session_description::RTCSessionDescription}, rtp_transceiver::rtp_codec::RTPCodecType};
use crate::{IceRestartExt, Storage, SyncChannel, actor::{Actor, Addr, Ctx, StoppingExt, WeakAddr}, audio_packet_forwarder::AudioPacketForwarder, create_peer, error::Error, keyframe_interceptor::{KeyframeInterceptor, RequestKeyframe}, quality_monitor::{DeviceType, QualityMonitor, QualityThresholds}, room::{AudioRouterStream, Codec, Room, RoomMessage, StreamQuality, VideoRouterStream}, rtp_packet_gateway_router::{AudioRouter, AudioRouterContext, RouterContext, RtpPacketGatewayRouter, RtpPacketMessage, VideoRouter, VideoRouterContext}, user::{IceCandidate, MessageType, SignalMessage, Target, User, UserMessage, initiate_ice_restart}, video_packet_forwarder::VideoPacketForwarder};

pub struct Publisher<C: SyncChannel, S: Storage> {
    pub peer_id: Uuid,
    pub pc: Arc<RTCPeerConnection>,
    pub user: Addr<User<C, S>>,
    pub room: Addr<Room<C, S>>,    
    pub video_track_keyframe_interceptors: Vec<Addr<KeyframeInterceptor>>,
    pub qualify_monitor: WeakAddr<QualityMonitor<C, S>>,
    pub ice_candidate_send: bool,
    pub disconnected: bool,
    pub retry_connect_attempts: u8,
}


const TARGET: Target = Target::Publisher;

impl<C: SyncChannel, S: Storage> Publisher<C, S> {
    pub async fn new(user: Addr<User<C, S>>, room: Addr<Room<C, S>>, peer_id: Uuid) -> Result<Self, Error> {
        let peer = create_peer().await?;
        let pc = Arc::new(peer);
        Ok(Self {
            pc,
            user,
            room,
            peer_id,
            qualify_monitor: WeakAddr::default(),
            disconnected: false,
            retry_connect_attempts: 0,
            ice_candidate_send: false,
            video_track_keyframe_interceptors: Vec::with_capacity(3)
        })
    }
    pub async fn try_stop(self) -> Result<(), Error> {
        self.qualify_monitor.try_terminate().await?;
        self.pc.close().await?;
        Ok(())
    }
}

pub enum PublisherMessage {
    Websocket (MessageType),
    IceStateChange {
        state: RTCIceConnectionState,
    },
    CheckIceState,
    IceCandidate { 
        candidate: IceCandidate,
    },
    InitiateMonitoring { device_type: DeviceType },
    NewAudioTrack(AudioRouterStream),
    NewVideoTrack{ quality: StreamQuality, video_router_stream: VideoRouterStream },
}

impl From<RTCIceConnectionState> for PublisherMessage {
    fn from(value: RTCIceConnectionState) -> Self {
        Self::IceStateChange { state: value }
    }
}

impl<C: SyncChannel, S: Storage> Actor for Publisher<C, S> {
    type Message = PublisherMessage;
    async fn handle(&mut self, ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            PublisherMessage::CheckIceState => {
                self.check_ice_state(ctx).await.ok_or_terminate(ctx);
            }
            PublisherMessage::IceStateChange { state } => {
                let addr = ctx.addr.clone();
                self.handle_ice_state_change(&addr, state).await.ok_or_terminate(ctx);
            }
            PublisherMessage::NewAudioTrack(router) => {
                self.room.send(RoomMessage::AddAudioTrack { 
                        peer_id: self.peer_id, 
                        router, 
                })
                .await
                .ok_or_terminate(ctx);
            }
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
            PublisherMessage::IceCandidate { candidate } => if !self.ice_candidate_send {
               let message = SignalMessage::Rtc {
                    target: TARGET,
                    message_type: MessageType::Candidate { candidate }
                };
                self.user.send(UserMessage::SignalMessage(message)).await.ok_or_terminate(ctx);
                self.ice_candidate_send = true;
            },
            PublisherMessage::InitiateMonitoring { device_type } => {
                self.initiate_monitoring(device_type).await.ok_or_terminate(ctx);
            }
        }
    }   
    async fn starting(&mut self, ctx: &Ctx<'_, Self>) {
        let addr = ctx.addr.clone();
        self.pc.on_ice_candidate(Box::new(move |candidate| {
            let addr = addr.clone();
            Box::pin(async move {
                if let Some(cand) = candidate {
                    if let Ok(RTCIceCandidateInit { candidate, sdp_mid, sdp_mline_index, .. }) = cand.to_json() {
                        let _ = addr.send(PublisherMessage::IceCandidate { candidate: IceCandidate { candidate, sdp_mid, sdp_mline_index } }).await;
                    }
                }
            })
        }));
        let addr = ctx.addr.clone();
        self.on_ice_connection_state_change(addr);
        let addr = ctx.addr.clone();
        let pc = self.pc.clone();
        self.pc.on_track(Box::new(move |track, _, _| {
            let pc = pc.clone();
            let addr = addr.clone();
            let ssrc = track.ssrc();
            let mime_type = track.codec().capability.mime_type;
            let mime_type = Codec::from_str(&mime_type).unwrap_or_default();
            let kind = track.kind();
            let rid = StreamQuality::from_str(track.rid());
            Box::pin(async move {
                match kind {
                    RTPCodecType::Audio | RTPCodecType::Unspecified => { 
                        let rtp_packet_forwarder= RtpPacketGatewayRouter::<AudioPacketForwarder, AudioRouterContext>::spawn(track, AudioRouterContext {});
                        addr.send(
                            PublisherMessage::NewAudioTrack(AudioRouterStream { router: rtp_packet_forwarder, codek: mime_type })
                        )
                        .await
                        .ok();
                    },
                    RTPCodecType::Video => {
                        let keyframe_interceptor = KeyframeInterceptor::new(pc.clone(), ssrc).start();
                        let quality = rid.unwrap_or(StreamQuality::High);
                        let (wake_rx, wake_tx) = tokio::sync::broadcast::channel(1);
                        let context = VideoRouterContext::new(pc, quality, ssrc, wake_rx);   
                        let rtp_packet_forwarder= RtpPacketGatewayRouter::<VideoPacketForwarder, VideoRouterContext>::spawn(track, context);
                        addr.send(
                            PublisherMessage::NewVideoTrack {
                                quality,
                                video_router_stream: VideoRouterStream { router: 
                                    rtp_packet_forwarder, 
                                    codek: mime_type, 
                                    keyframe_interceptor, 
                                    wake_tx: Arc::new(wake_tx) 
                                }
                            }
                        )
                        .await
                        .ok();
                    }   
                };
            })
        }));
    }
    async fn stopping(self, _: &Ctx<'_, Self>) {
        if let Err(e) = self.try_stop().await {
            tracing::error!("[Publisher] stopping {e}");
        }
        tracing::info!("🔴 [Publisher] Пользователь уничтожен.");
    }
}

impl<C: SyncChannel, S: Storage> Publisher<C, S> {
    async fn initiate_monitoring(&mut self, device_type: DeviceType) -> Result<(), Error> {
        let quality_monitor = QualityMonitor::new(
            self.pc.clone(), 
            self.user.clone(), 
            QualityThresholds::from(device_type)
        )
            .await?
            .start();
        self.qualify_monitor.set_addr(quality_monitor);
        Ok(())
    }
    async fn handle_ws_message(&mut self, message: MessageType) -> Result<(), Error> {
        match message {
            MessageType::Offer {sdp} => {
                let offer_desc = RTCSessionDescription::offer(sdp)?;
                self.pc.set_remote_description(offer_desc).await?;
                let answer = self.pc.create_answer(None).await?;
                self.pc.set_local_description(answer.clone()).await?;
                let message = SignalMessage::Rtc {
                    target: Target::Publisher,
                    message_type: MessageType::Answer {sdp: answer.sdp }
                };
                let _ = self.user.send(UserMessage::SignalMessage(message)).await;
                let _ = self.room.send(RoomMessage::SubscribeToPeers { peer_id: self.peer_id }).await;
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
            _ => {}
        }
        Ok(())
    }
}

impl<C: SyncChannel, S: Storage> IceRestartExt for Publisher<C, S> {
    const CHECK_ICE_STATE: Self::Message = PublisherMessage::CheckIceState;
    const TARGET: Target = Target::Publisher;
    async fn on_reconnect(&self) -> Result<(), Error> {
        tracing::info!("try reconnect");
        for interceptor in &self.video_track_keyframe_interceptors {
            interceptor.send(RequestKeyframe::Fir).await?;
        }
        Ok(())
    }
    fn disconnected(&mut self) -> &mut bool {
        &mut self.disconnected
    }
    fn peer_connection(&self) -> &RTCPeerConnection {
        &self.pc
    }
    fn retry_connect_attempts(&mut self) -> &mut u8 {
        &mut self.retry_connect_attempts
    }
    async fn send_target_message(&self, message: SignalMessage) -> Result<(), Error> {
        self.user.send(UserMessage::SignalMessage(message)).await?;
        Ok(())
    }
}