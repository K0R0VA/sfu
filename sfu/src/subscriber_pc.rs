use std::{collections::HashMap, sync::{Arc}};
use uuid::Uuid;
use webrtc::{ice_transport::{ice_candidate::RTCIceCandidateInit, ice_connection_state::RTCIceConnectionState}, peer_connection::{RTCPeerConnection, sdp::session_description::RTCSessionDescription, signaling_state::RTCSignalingState}};
use crate::{PacketAudioSubscription, PacketVideoSubscription, SyncChannel, actor::{Actor, Addr, Ctx}, audio_subscription::AudioSubscription, create_peer, error::Error, quality_monitor::QualityMonitor, room::StreamQuality, user::{ConnectionRequest, IceCandidate, MessageType, SignalMessage, Target, User, UserMessage, initiate_ice_restart}, video_subscription::{VideoSubscription, VideoSubscriptionMessage}};

pub struct Subscriber<S: SyncChannel> {
    pub user: Addr<User<S>>,
    pub pc: Arc<RTCPeerConnection>,
    pub audio_subscriptions: HashMap<Uuid, Addr<AudioSubscription<S>>>,
    pub video_subscriptions: HashMap<Uuid, Addr<VideoSubscription>>,
    pub qualify_monitor: Option<Addr<QualityMonitor<S>>>,
    pub disconnected: bool,
    pub retry_connect_attempts: u8,
}

const TARGET: Target = Target::Subscriber;

impl<S: SyncChannel> Subscriber<S> {
    pub async fn new(user: Addr<User<S>>) -> Result<Self, Error> {
        let pc = create_peer().await?;
        let pc = Arc::new(pc);
        Ok(Self {
            user,
            pc,
            audio_subscriptions: HashMap::new(),
            video_subscriptions: HashMap::new(),
            qualify_monitor: None,
            disconnected: false,
            retry_connect_attempts: 0
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
    ConnectAudio(ConnectionRequest<PacketAudioSubscription>),
    ConnectVideo { request: ConnectionRequest<PacketVideoSubscription>, quality: StreamQuality },
    Unsubscribe { peer_id: Uuid }
}

impl<S: SyncChannel> Actor for Subscriber<S> {
    type Message = SubscriberMessage;
    async fn handle(&mut self, ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            SubscriberMessage::CheckIceState => {
                let current_state = self.pc.ice_connection_state();
                let is_disconnected = [
                        RTCIceConnectionState::Failed, 
                        RTCIceConnectionState::Disconnected
                    ]
                    .iter()
                    .any(|failed_state| *failed_state == current_state);
                if is_disconnected {
                    if self.retry_connect_attempts < 3 {
                        let addr = ctx.addr.clone();
                        tokio::spawn(async move {
                            let _ = addr.send(SubscriberMessage::CheckIceState).await;
                        });
                    } else {
                        self.stop(ctx).await;
                    }
                }
            }
            SubscriberMessage::IceStateChange { state } => {
                if let Err(e) = self.handle_ice_state_change(state).await {
                    tracing::error!("[UserActor] UserMessage::Signal {e}");
                }
            }
            SubscriberMessage::SwitchQualityLayer { quality } => {
                for subscription in self.video_subscriptions.values() {
                    let _ = subscription.send(VideoSubscriptionMessage::SwitchQualityLayer { to: quality }).await;
                }
            }
            SubscriberMessage::Signal { offer: sdp } => {
                let message = SignalMessage::Rtc {
                    target: TARGET,
                    message_type: MessageType::Offer { sdp }
                };
                let _ = self.user.send(UserMessage::SignalMessage(message)).await;
            }
            SubscriberMessage::IceCandidate { candidate } => {
                let message = SignalMessage::Rtc {
                    target: TARGET,
                    message_type: MessageType::Candidate { candidate }
                };
                let _ = self.user.send(UserMessage::SignalMessage(message)).await;
            }
            SubscriberMessage::Websocket (message)=> {
                if let Err(e) = self.handle_ws_message(message).await {
                    tracing::error!("👤 [UserActor] UserMessage::Websocket {:?}", e);
                    self.stop(ctx).await;
                }
            },
            SubscriberMessage::ConnectAudio(request) => {
                if let Err(e) = self.connect_audio(request).await {
                    tracing::error!("👤 [UserActor] UserMessage::ConnectToUser {:?}", e);
                    self.stop(ctx).await;
                }
            },
            SubscriberMessage::ConnectVideo {quality, request} => {
                if let Err(e) = self.connect_video(quality, request).await {
                    tracing::error!("👤 [UserActor] UserMessage::ConnectToUser {:?}", e);
                    self.stop(ctx).await;
                }
            },
            SubscriberMessage::Unsubscribe { peer_id: from } => {
                if let Err(e) = self.disconnect_from_user(from).await {
                    tracing::error!("👤 [UserActor] UserMessage::DisconnectFromUser {:?}", e);
                    self.stop(ctx).await;
                }
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
            sub.terminate().await;
        }
        for sub in self.video_subscriptions.values() {
            sub.terminate().await;
        }
        tracing::info!("🔴 [UserActor] Пользователь уничтожен.");
    }
}

impl<S: SyncChannel> Subscriber<S> {
    async fn handle_ice_state_change(&mut self, state: RTCIceConnectionState) -> Result<(), Error> {
        match state {
            RTCIceConnectionState::Disconnected | RTCIceConnectionState::Failed => {
                tracing::warn!("[Subscriber] IceStateChange");
                self.disconnected = true;
                let message = initiate_ice_restart(&self.pc, Target::Subscriber).await?;
                let _ = self.user.send(UserMessage::SignalMessage(message)).await;
                self.retry_connect_attempts += 1;
            },
            RTCIceConnectionState::Connected if self.disconnected => {
                self.disconnected = false;
                self.retry_connect_attempts = 0;
            },
            _ => {}
        }
        Ok(())
    }
    
    async fn handle_ws_message(&mut self, message: MessageType) -> Result<(), Error> {
        match message {
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
    async fn connect_audio(&mut self, request: ConnectionRequest<PacketAudioSubscription>) -> Result<(), Error> {
        let peer_id = request.peer_id;
        let audio_subscription = AudioSubscription::init(self.pc.clone(), self.user.clone(), request).await?;
        let audio_subscription = audio_subscription.start();
        self.audio_subscriptions.insert(peer_id, audio_subscription);
        Ok(())
    }
    async fn connect_video(&mut self, quality: StreamQuality, request: ConnectionRequest<PacketVideoSubscription>) -> Result<(), Error> {
        let peer_id = request.peer_id;
        match self.video_subscriptions.entry(peer_id) {
            std::collections::hash_map::Entry::Occupied(o) => {
                let addr = o.get();
                let _ = addr.send(VideoSubscriptionMessage::AddSubsription { quality, stream: request.stream }).await;
            },
            std::collections::hash_map::Entry::Vacant(v) => {
                let video_subscription = VideoSubscription::new(
                    self.pc.clone(), 
                    peer_id, 
                    request.codec_mime_type, 
                    request.stream, 
                    quality
                ).await?;
                let video_subscription = video_subscription.start();
                v.insert(video_subscription);
            }
        };
        Ok(())
    }
    async fn disconnect_from_user(&mut self, speaker_id: Uuid) -> Result<(), Error> {
        let audio_subscription = self.audio_subscriptions
            .remove(&speaker_id)
            .ok_or(Error::SystemError { message: "subscription not found".into() })?;
        audio_subscription.terminate().await;
        let video_subscription = self.video_subscriptions
            .remove(&speaker_id)
            .ok_or(Error::SystemError { message: "subscription not found".into() })?;
        video_subscription.terminate().await;
        Ok(())
    }
}