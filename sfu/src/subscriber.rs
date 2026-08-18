use std::{collections::HashMap};
use rtc::{rtp_transceiver::RTCRtpTransceiverDirection};
use tokio::sync::mpsc::Receiver;
use uuid::Uuid;
use webrtc::peer_connection::{PeerConnection, PeerConnectionEventHandler, RTCIceCandidateInit, RTCPeerConnectionIceEvent, RTCSessionDescription, RTCSignalingState};
use crate::{SignalingClient, Storage, actor::{Actor, Addr, Ctx, StoppingExt}, audio_packet_forwarder::AudioPacketForwarder, create_peer, error::Error, keyframe_interceptor::KeyframeInterceptor, room::StreamQuality, rtp_packet_gateway_router::{AudioRouterContext, RouterWaker, VideoRouterContext}, server::Key, subscriber::SubscriberMessage::OnNegotiationNeeded, user::{ConnectionRequest, IceCandidate, MessageType, SignalMessage, Target, User, UserMessage}, video_layer_manager::{QualityLayer, VideoLayerManager, VideoLayerManagerMessage}, video_packet_forwarder::VideoPacketForwarder};

pub struct Subscriber<K: Key, C: SignalingClient<UserKey = K>, S: Storage> {
    pub user: Addr<User<K, C, S>>,
    pub pc: Box<dyn PeerConnection>,
    pub audio_subscriptions: HashMap<K, Addr<AudioPacketForwarder>>,
    pub video_subscriptions: HashMap<K, Addr<VideoLayerManager>>,
    pub current_quality: StreamQuality,
    pub rx: Option<Receiver<SubscriberMessage<K>>>,
    pub signaling_state: Option<RTCSignalingState>,
    pub should_send_offer: bool,
}

const TARGET: Target = Target::Subscriber;

impl<K: Key, C: SignalingClient<UserKey = K>, S: Storage> Subscriber<K, C, S> {
    pub async fn new(user: Addr<User<K, C, S>>) -> Result<Self, Error> {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let pc = create_peer(SubscriberPeerConnectionHandler {tx}).await?;
        Ok(Self {
            user,
            pc,
            audio_subscriptions: HashMap::new(),
            video_subscriptions: HashMap::new(),
            current_quality: StreamQuality::High,
            rx: Some(rx),
            signaling_state: None,
            should_send_offer: false
        })
    }
}

pub enum SubscriberMessage<K: Key> {
    SwitchQualityLayer { quality: StreamQuality },
    IceCandidate { 
        candidate: IceCandidate,
    },
    SignalingStateChanged (RTCSignalingState),
    Websocket (MessageType),
    ConnectAudio(ConnectionRequest<K, AudioPacketForwarder, AudioRouterContext>),
    ConnectVideo { 
        request: ConnectionRequest<K, VideoPacketForwarder, VideoRouterContext>, 
        quality: StreamQuality, 
        keyframe_interceptor: Addr<KeyframeInterceptor>, 
        wake_notification: RouterWaker
    },
    Unsubscribe { peer_id: K },
    OnNegotiationNeeded
}

impl<K: Key, C: SignalingClient<UserKey = K>, S: Storage> Actor for Subscriber<K, C, S> {
    type Message = SubscriberMessage<K>;
    async fn handle(&mut self, ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            SubscriberMessage::SignalingStateChanged(state) => {
                if self.should_send_offer && state == RTCSignalingState::Stable {
                    self.send_offer().await.ok_or_terminate(ctx);
                }
                self.signaling_state = Some(state);
            }
            SubscriberMessage::SwitchQualityLayer { quality } => {
                self.current_quality = quality;
                for subscription in self.video_subscriptions.values() {
                    subscription.send(VideoLayerManagerMessage::SwitchQualityLayer { to: quality }).await.ok_or_terminate(ctx);
                }
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
            SubscriberMessage::OnNegotiationNeeded => {
                self.send_offer().await.ok_or_terminate(ctx);
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
        for sub in self.audio_subscriptions.values() {
            sub.terminate().await.ok();
        }
        for sub in self.video_subscriptions.values() {
            sub.terminate().await.ok();
        }
        tracing::info!("🔴 [UserActor] Пользователь уничтожен.");
    }
}

impl<K: Key, C: SignalingClient<UserKey = K>, S: Storage> Subscriber<K, C, S> {
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
    async fn send_offer(&mut self) -> Result<(), Error> {
        if self.signaling_state.is_some() && self.signaling_state != Some(RTCSignalingState::Stable) { 
            self.should_send_offer = true;
            return Ok(()); 
        }
        let offer = self.pc.create_offer(None).await?;
        self.pc.set_local_description(offer.clone()).await?;
        let message = SignalMessage::Rtc {
            target: TARGET,
            message_type: MessageType::Offer { sdp: offer.sdp }
        };
        self.user.send(UserMessage::SignalMessage(message)).await?;
        Ok(())
    }
    async fn connect_audio(&mut self, request: ConnectionRequest<K, AudioPacketForwarder, AudioRouterContext>) -> Result<(), Error> {
        let peer_id = request.peer_id;
        let audio_subscription = AudioPacketForwarder::init(&self.pc, request).await?;
        let audio_subscription = audio_subscription.start();
        self.audio_subscriptions.insert(peer_id, audio_subscription);
        Ok(())
    }
    async fn connect_video(&mut self, 
        quality: StreamQuality, 
        request: ConnectionRequest<K, VideoPacketForwarder, VideoRouterContext>, 
        keyframe_interceptor: Addr<KeyframeInterceptor>,
        wake_notification: RouterWaker
    ) -> Result<(), Error> {
        let peer_id = request.peer_id;
        let layer = QualityLayer { gateway_router: request.gateway_router, keyframe_interceptor, router_waker: wake_notification };
        match self.video_subscriptions.entry(peer_id) {
            std::collections::hash_map::Entry::Occupied(o) => {
                let addr = o.get();
                addr.send(VideoLayerManagerMessage::AddLayer { quality, layer  }).await?;
            },
            std::collections::hash_map::Entry::Vacant(v) => {
                let video_subscription = VideoLayerManager::new(
                    &mut self.pc, 
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
    async fn disconnect_from_user(&mut self, speaker_id: K) -> Result<(), Error> {
        if let Some(audio_subscriptions) = self.audio_subscriptions.remove(&speaker_id) {
            audio_subscriptions.terminate().await?;
        };
        if let Some(video_subscription) = self.video_subscriptions.remove(&speaker_id) {
            video_subscription.terminate().await?;
        };
        Ok(())
    }
}

pub struct SubscriberPeerConnectionHandler<K: Key> {
    tx: tokio::sync::mpsc::Sender<SubscriberMessage<K>>
}

#[async_trait::async_trait]
impl<K: Key> PeerConnectionEventHandler for SubscriberPeerConnectionHandler<K> {
    async fn on_ice_candidate(&self, event: RTCPeerConnectionIceEvent) {
        if let Ok(RTCIceCandidateInit { candidate, sdp_mid, sdp_mline_index, .. }) = event.candidate.to_json() {
            self.tx.send(SubscriberMessage::IceCandidate { candidate: IceCandidate { candidate, sdp_mid, sdp_mline_index } })
                .await
                .expect("on_ice_candidate");
        }
    }
    async fn on_signaling_state_change(&self, state: RTCSignalingState) {
        self.tx.send(SubscriberMessage::SignalingStateChanged(state)).await.expect("on_signaling_state_change");
    }
    async fn on_negotiation_needed(&self) {
        self.tx.send(SubscriberMessage::OnNegotiationNeeded).await.expect("on_negotiation_needed");
    }
}