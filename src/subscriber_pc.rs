use std::{collections::HashMap, sync::{Arc}};
use uuid::Uuid;
use webrtc::{api::{APIBuilder, interceptor_registry::register_default_interceptors, media_engine::MediaEngine, setting_engine::SettingEngine}, ice_transport::{ice_candidate::RTCIceCandidateInit, ice_connection_state::RTCIceConnectionState, ice_server::RTCIceServer}, interceptor::registry::Registry, peer_connection::{RTCPeerConnection, configuration::RTCConfiguration, sdp::session_description::RTCSessionDescription, signaling_state::RTCSignalingState}, rtp_transceiver::rtp_codec::{RTCRtpHeaderExtensionCapability, RTPCodecType}};
use crate::{PacketAudioSubscription, PacketVideoSubscription, actor::{Actor, Addr, Ctx}, audio_subscription::AudioSubscription, error::Error, quality_monitor::{QualityMonitor}, room::{StreamQuality}, user::{ConnectionRequest, IceCandidate, MessageType, SignalMessage, Target, User, UserMessage}, video_subscription::{VideoSubscription, VideoSubscriptionMessage}};

pub struct Subscriber {
    pub user: Addr<User>,
    pub pc: Arc<RTCPeerConnection>,
    pub audio_subscriptions: HashMap<Uuid, Addr<AudioSubscription>>,
    pub video_subscriptions: HashMap<Uuid, Addr<VideoSubscription>>,
    pub qualify_monitor: Option<Addr<QualityMonitor>>
}

const TARGET: Target = Target::Subscriber;

impl Subscriber {
    pub async fn new(user: Addr<User>) -> Result<Self, Error> {
        let mut m = MediaEngine::default();
        for uri in [
            "urn:ietf:params:rtp-hdrext:sdes:mid",
            "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id",
            "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id",
        ] {
            m.register_header_extension(
                RTCRtpHeaderExtensionCapability {
                    uri: uri.to_owned(),
                },
                    RTPCodecType::Video,
                None,
            )?;
        }

        m.register_default_codecs()?;
        
    // Регистрируем этот кодек на прием и на отправку
    
        let registry = register_default_interceptors(Registry::new(), &mut m)?;
        let mut system_engine = SettingEngine::default();
        system_engine
            .set_interface_filter(
                Box::new(|iface|{
                    !iface.starts_with("docker") && !iface.starts_with("br-") && !iface.starts_with("veth")
                })
            );
        // Настраиваем API WebRTC
        let api = APIBuilder::new()
            .with_media_engine(m)
            .with_interceptor_registry(registry)
            .with_setting_engine(system_engine)
            .build();
        // 2. STUN/TURN Сервера (Проблемное место #1: NAT Traversal)
        let config = RTCConfiguration {
            ice_servers: vec![
                RTCIceServer {
                    urls: vec![
                        "stun:stun.l.google.com:19302".to_string(),
                    ],
                    ..Default::default()
                }
            ],
            ..Default::default()
        };
        // 3. Создаем PeerConnection
        let pc = Arc::new(api.new_peer_connection(config.clone()).await?);
        Ok(Self {
            user,
            pc,
            audio_subscriptions: HashMap::new(),
            video_subscriptions: HashMap::new(),
            qualify_monitor: None
        })
    }
}

pub enum SubscriberMessage {
    SwitchQualityLayer { quality: StreamQuality },
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
    Unsubscribe { from: Uuid }
}

impl Actor for Subscriber {
    type Message = SubscriberMessage;
    async fn handle(&mut self, ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            SubscriberMessage::IceStateChange { state } => {
                // if let Err(e) = self.handle_ice_state_change(state, target).await {
                //     tracing::error!("[UserActor] UserMessage::Signal {e}");
                // }
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
                if let Err(e) = self.handle_ws_message(ctx, message).await {
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
            SubscriberMessage::Unsubscribe { from } => {
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
        tracing::info!("🟢 [Userctor] Пользователь инициализирован.");
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

impl Subscriber {
    async fn handle_ice_state_change(&mut self, ctx: &mut Ctx<'_, Self>, state: RTCIceConnectionState, target: Target) -> Result<(), Error> {
        match (state, target) {
            (RTCIceConnectionState::Failed, target) => {
                // self.initiate_ice_restart(target).await?;
            },
            (RTCIceConnectionState::Disconnected, Target::Publisher) => {
            },
            (RTCIceConnectionState::Disconnected, Target::Subscriber) => {},
            (RTCIceConnectionState::Connected, Target::Publisher) => {},
            (RTCIceConnectionState::Connected, Target::Subscriber) => {},
            _ => {}
        }
        Ok(())
    }
    async fn handle_ws_message(&mut self, ctx: &mut Ctx<'_, Self>, message: MessageType) -> Result<(), Error> {
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
        let peer_id = request.speaker_id;
        let audio_subscription = AudioSubscription::init(self.pc.clone(), self.user.clone(), request).await?;
        let audio_subscription = audio_subscription.start();
        self.audio_subscriptions.insert(peer_id, audio_subscription);
        Ok(())
    }
    async fn connect_video(&mut self, quality: StreamQuality, request: ConnectionRequest<PacketVideoSubscription>) -> Result<(), Error> {
        let peer_id = request.speaker_id;
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
        // let notice = serde_json::json!({
        //     "type": "peer_left",
        //     "peer_id": speaker_id
        // });
        // self.ws_tx.send(Message::Text(notice.to_string().into())).await?;
        Ok(())
    }
}