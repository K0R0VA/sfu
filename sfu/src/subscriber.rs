use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;
use webrtc::{ice_transport::{ice_candidate::RTCIceCandidateInit, ice_connection_state::RTCIceConnectionState}, peer_connection::{RTCPeerConnection, sdp::session_description::RTCSessionDescription, signaling_state::RTCSignalingState}};
use crate::{IceRestartExt, Storage, SignalingClient, actor::{Actor, Addr, Ctx, StoppingExt}, audio_packet_forwarder::AudioPacketForwarder, create_peer, error::Error, keyframe_interceptor::KeyframeInterceptor, room::StreamQuality, rtp_packet_gateway_router::{AudioRouterContext, RouterWaker, VideoRouterContext}, user::{ConnectionRequest, IceCandidate, MessageType, SignalMessage, Target, User, UserMessage}, video_layer_manager::{QualityLayer, VideoLayerManager, VideoLayerManagerMessage}, video_packet_forwarder::VideoPacketForwarder};

pub struct Subscriber<C: SignalingClient, S: Storage> {
    pub user: Addr<User<C, S>>,
    pub pc: Arc<RTCPeerConnection>,
    pub audio_subscriptions: HashMap<Uuid, Addr<AudioPacketForwarder>>,
    pub video_subscriptions: HashMap<Uuid, Addr<VideoLayerManager>>,
    pub current_quality: StreamQuality,
    pub disconnected: bool,
    pub retry_connect_attempts: u8,
}

const TARGET: Target = Target::Subscriber;

impl<C: SignalingClient, S: Storage> Subscriber<C, S> {
    pub async fn new(user: Addr<User<C, S>>) -> Result<Self, Error> {
        let pc = create_peer().await?;
        let pc = Arc::new(pc);
        Ok(Self {
            user,
            pc,
            audio_subscriptions: HashMap::new(),
            video_subscriptions: HashMap::new(),
            disconnected: false,
            retry_connect_attempts: 0,
            current_quality: StreamQuality::High,
        })
    }
}

pub enum SubscriberMessage {
    SwitchQualityLayer { quality: StreamQuality },
    CheckIceState,
    Signal {
        offer: String
    },
    IceStateChange {
        state: RTCIceConnectionState,
    },
    IceCandidate { 
        candidate: IceCandidate,
    },
    Websocket (MessageType),
    ConnectAudio(ConnectionRequest<AudioPacketForwarder, AudioRouterContext>),
    ConnectVideo { 
        request: ConnectionRequest<VideoPacketForwarder, VideoRouterContext>, 
        quality: StreamQuality, 
        keyframe_interceptor: Addr<KeyframeInterceptor>, 
        wake_notification: RouterWaker
    },
    Unsubscribe { peer_id: Uuid }
}

impl From<RTCIceConnectionState> for SubscriberMessage {
    fn from(value: RTCIceConnectionState) -> Self {
        Self::IceStateChange { state: value }
    }
}

impl<C: SignalingClient, S: Storage> Actor for Subscriber<C, S> {
    type Message = SubscriberMessage;
    async fn handle(&mut self, ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            SubscriberMessage::CheckIceState => { self.check_ice_state(ctx).await.ok_or_terminate(ctx); },
            SubscriberMessage::IceStateChange { state } => {
                let addr = ctx.addr.clone();
                self.handle_ice_state_change(&addr, state).await.ok_or_terminate(ctx);
            }
            SubscriberMessage::SwitchQualityLayer { quality } => {
                self.current_quality = quality;
                for subscription in self.video_subscriptions.values() {
                    subscription.send(VideoLayerManagerMessage::SwitchQualityLayer { to: quality }).await.ok_or_terminate(ctx);
                }
            }
            SubscriberMessage::Signal { offer: sdp } => {
                let message = SignalMessage::Rtc {
                    target: TARGET,
                    message_type: MessageType::Offer { sdp }
                };
                self.user.send(UserMessage::SignalMessage(message)).await.ok_or_terminate(ctx);
            }
            SubscriberMessage::IceCandidate { candidate } => {
                let message = SignalMessage::Rtc {
                    target: TARGET,
                    message_type: MessageType::Candidate { candidate }
                };
                self.user.send(UserMessage::SignalMessage(message)).await.ok_or_terminate(ctx);
            }
            SubscriberMessage::Websocket (message)=> {
                self.handle_ws_message(message).await.ok_or_terminate(ctx);
            },
            SubscriberMessage::ConnectAudio(request) => {
                self.connect_audio(request).await.ok_or_terminate(ctx);
            },
            SubscriberMessage::ConnectVideo {quality, request, keyframe_interceptor, wake_notification} => {
               self.connect_video(quality, request, keyframe_interceptor, wake_notification).await.ok_or_terminate(ctx);
            },
            SubscriberMessage::Unsubscribe { peer_id: from } => {
               self.disconnect_from_user(from).await.ok_or_terminate(ctx);
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
                        let _ = addr.send(SubscriberMessage::IceCandidate { candidate: IceCandidate { candidate, sdp_mid, sdp_mline_index } }).await;
                    }
                }
            })
        }));
        let addr = ctx.addr.clone();
        self.on_ice_connection_state_change(addr);
        let addr = ctx.addr.clone();
        let pc = Arc::clone(&self.pc);
        self.pc.on_negotiation_needed(Box::new(move || {
            let pc = Arc::clone(&pc);
            let addr = addr.clone();
            Box::pin(async move {
                if pc.signaling_state() != RTCSignalingState::Stable {
                    return;
                }
                match pc.create_offer(None).await {
                    Ok(offer) => {
                        if let Err(e) = pc.set_local_description(offer.clone()).await {
                            tracing::error!("[User] set_local_description: {:?}", e);
                            return;
                        }
                        let _ = addr.send(SubscriberMessage::Signal {
                            offer: offer.sdp
                        }).await;
                    }
                    Err(e) => tracing::error!("[User] create_offer: {:?}", e),
                }
            })
        }));
    }
    async fn stopping(self, _: &Ctx<'_, Self>) {
        for sub in self.audio_subscriptions.values() {
            sub.terminate().await.ok();
        }
        for sub in self.video_subscriptions.values() {
            sub.terminate().await.ok();
        }
        tracing::info!("🔴 [UserActor] Пользователь уничтожен.");
    }
}

impl<C: SignalingClient, S: Storage> Subscriber<C, S> {
    async fn handle_ws_message(&mut self, message: MessageType) -> Result<(), Error> {
        match message {
            MessageType::IceRestart {sdp} => {
                let offer_desc = RTCSessionDescription::offer(sdp)?;
                self.pc.set_remote_description(offer_desc).await?;
                let answer = self.pc.create_answer(None).await?;
                self.pc.set_local_description(answer.clone()).await?;
                let message = SignalMessage::Rtc {
                    target: Target::Subscriber,
                    message_type: MessageType::Answer {sdp: answer.sdp }
                };
                let _ = self.user.send(UserMessage::SignalMessage(message)).await;
            },
            MessageType::Answer { sdp } => {
                let answer_desc = RTCSessionDescription::answer(sdp)?;
                self.pc.set_remote_description(answer_desc).await?;
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
            r#type => {
                tracing::warn!("unimplemented {:?}", r#type);
            }
        }
        Ok(())
    }
    async fn connect_audio(&mut self, request: ConnectionRequest<AudioPacketForwarder, AudioRouterContext>) -> Result<(), Error> {
        let peer_id = request.peer_id;
        let audio_subscription = AudioPacketForwarder::init(self.pc.clone(), request).await?;
        let audio_subscription = audio_subscription.start();
        self.audio_subscriptions.insert(peer_id, audio_subscription);
        Ok(())
    }
    async fn connect_video(&mut self, 
        quality: StreamQuality, 
        request: ConnectionRequest<VideoPacketForwarder, VideoRouterContext>, 
        keyframe_interceptor: Addr<KeyframeInterceptor>,
        wake_notification: RouterWaker
    ) -> Result<(), Error> {
        let peer_id = request.peer_id;
        let layer = QualityLayer { gateway_router: request.gateway_router, keyframe_interceptor, router_waker: wake_notification };
        match self.video_subscriptions.entry(peer_id) {
            std::collections::hash_map::Entry::Occupied(o) => {
                let addr = o.get();
                let _ = addr.send(VideoLayerManagerMessage::AddLayer { quality, layer  }).await;
            },
            std::collections::hash_map::Entry::Vacant(v) => {
                let video_subscription = VideoLayerManager::new(
                    self.pc.clone(), 
                    peer_id, 
                    request.codec_mime_type, 
                    self.current_quality,
                    quality,
                    layer, 
                ).await?;
                let video_subscription = video_subscription.start();
                v.insert(video_subscription);
            }
        };
        Ok(())
    }
    async fn disconnect_from_user(&mut self, speaker_id: Uuid) -> Result<(), Error> {
        if let Some(audio_subscriptions) = self.audio_subscriptions.remove(&speaker_id) {
            audio_subscriptions.terminate().await?;
        };
        if let Some(video_subscription) = self.video_subscriptions.remove(&speaker_id) {
            video_subscription.terminate().await?;
        };
        Ok(())
    }
}

impl<C: SignalingClient, S: Storage> IceRestartExt for Subscriber<C, S> {
    const CHECK_ICE_STATE: Self::Message = SubscriberMessage::CheckIceState;
    const TARGET: Target = Target::Subscriber;
    async fn on_reconnect(&self) -> Result<(), Error> {
        for video_subscription in self.video_subscriptions.values() {
            video_subscription.send(VideoLayerManagerMessage::ResumeStreaming).await?;
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