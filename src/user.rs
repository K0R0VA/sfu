use std::{collections::HashMap, str::FromStr, sync::{Arc, atomic::AtomicUsize}, time::Duration};
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, stream::{SplitSink}};
use serde::{Deserialize, Serialize};
use tokio::time::interval;
use uuid::Uuid;
use webrtc::{api::{APIBuilder, interceptor_registry::register_default_interceptors, media_engine::MediaEngine, setting_engine::SettingEngine}, ice_transport::{ice_candidate::RTCIceCandidateInit, ice_server::RTCIceServer}, interceptor::registry::Registry, peer_connection::{RTCPeerConnection, configuration::RTCConfiguration, peer_connection_state::RTCPeerConnectionState, sdp::session_description::RTCSessionDescription, signaling_state::RTCSignalingState}, rtp_transceiver::rtp_codec::{RTCRtpHeaderExtensionCapability, RTPCodecType}};
use crate::{PacketAudioSubscription, PacketVideoSubscription, actor::{Actor, Addr, Ctx, StreamItem}, audio_subscription::AudioSubscription, error::Error, forward_rtp_packets, pli_sender::{Ping, PliSender}, quality_monitor::QualityMonitor, room::{PeerStream, Room, RoomMessage, StreamQuality}, video_subscription::{VideoSubscription, VideoSubscriptionMessage}};

pub struct User {
    pub room: Addr<Room>,
    pub peer_id: Uuid,
    pub ws_tx: SplitSink<WebSocket, Message>,
    pub publisher_pc: Arc<RTCPeerConnection>,
    pub subscriber_pc: Arc<RTCPeerConnection>,
    pub audio_subscriptions: HashMap<Uuid, Addr<AudioSubscription>>,
    pub video_subscriptions: HashMap<Uuid, Addr<VideoSubscription>>,
    pub qualify_monitor: Option<Addr<QualityMonitor>>
}

impl User {
    pub async fn new(ws_tx: SplitSink<WebSocket, Message>, room: Addr<Room>) -> Result<Self, Error> {
        let peer_id = Uuid::new_v4();
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
        let publisher_pc = Arc::new(api.new_peer_connection(config.clone()).await?);
        let subscriber_pc = Arc::new(api.new_peer_connection(config).await?);
        Ok(Self {
            publisher_pc,
            subscriber_pc,
            peer_id,
            room,
            ws_tx,
            audio_subscriptions: HashMap::new(),
            video_subscriptions: HashMap::new(),
            qualify_monitor: None
        })
    }
}

pub enum UserMessage {
    SwitchQualityLayer { quality: StreamQuality },
    Signal {
        offer: String
    },
    IceCandidate { 
        candidate: IceCandidate,
        target: Target 
    },
    Websocket {
        message: Result<Message, Error>
    },
    ConnectAudio(ConnectionRequest<PacketAudioSubscription>),
    ConnectVideo { request: ConnectionRequest<PacketVideoSubscription>, quality: StreamQuality },
    DisconnectFromUser {
        speaker_id: Uuid,
    },
    RoomClosed
}

pub struct ConnectionRequest<T> {
    pub speaker_id: Uuid,
    pub stream: T,
    pub codec_mime_type: String,
}

#[derive(Clone, Copy, Debug)]
pub enum ConnectionRequestKind {
    Audio,
    Video { stream_quality: StreamQuality }
}

impl Actor for User {
    type Message = UserMessage;
    async fn handle(&mut self, ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            UserMessage::SwitchQualityLayer { quality } => {
                for subscription in self.video_subscriptions.values() {
                    let _ = subscription.send(VideoSubscriptionMessage::SwitchQualityLayer { to: quality }).await;
                }
            }
            UserMessage::Signal { offer } => {
                if let Err(e) = self.send_offer(offer).await {
                    tracing::error!("[UserActor] UserMessage::Signal {e}");
                }
            }
            UserMessage::IceCandidate { candidate, target } => {
                if let Err(e) = self.send_ice_candidate(target, candidate).await {
                    tracing::error!("[UserActor] UserMessage::IceCandidate {e}");
                }
            }
            UserMessage::Websocket { message } => {
                if let Err(e) = self.handle_ws_message(message).await {
                    tracing::error!("👤 [UserActor] UserMessage::Websocket {:?}", e);
                    self.stop(ctx).await;
                }
            },
            UserMessage::ConnectAudio(request) => {
                if let Err(e) = self.connect_audio(ctx.addr.clone(), request).await {
                    tracing::error!("👤 [UserActor] UserMessage::ConnectToUser {:?}", e);
                    self.stop(ctx).await;
                }
            },
            UserMessage::ConnectVideo {quality, request} => {
                if let Err(e) = self.connect_video(quality, request).await {
                    tracing::error!("👤 [UserActor] UserMessage::ConnectToUser {:?}", e);
                    self.stop(ctx).await;
                }
            },
            UserMessage::RoomClosed => self.stop(ctx).await,
            UserMessage::DisconnectFromUser { speaker_id } => {
                if let Err(e) = self.disconnect_from_user(speaker_id).await {
                    tracing::error!("👤 [UserActor] UserMessage::DisconnectFromUser {:?}", e);
                    self.stop(ctx).await;
                }
            }
        }
    }   
    async fn starting(&mut self, ctx: &Ctx<'_, Self>) {
        self.qualify_monitor = Some(QualityMonitor::new(self.publisher_pc.clone(), ctx.addr.clone()).start());
        if let Err(e) = self.send_welcome().await {
            tracing::error!("[User] send_welcome {e}");
            return;
        }
        let addr = ctx.addr.clone();
        self.publisher_pc.on_ice_candidate(Box::new(move |candidate| {
            let addr = addr.clone();
            Box::pin(async move {
                if let Some(cand) = candidate {
                    if let Ok(RTCIceCandidateInit { candidate, sdp_mid, sdp_mline_index, .. }) = cand.to_json() {
                        let _ = addr.send(UserMessage::IceCandidate { candidate: IceCandidate { candidate, sdp_mid, sdp_mline_index }, target: Target::Publisher }).await;
                    }
                }
            })
        }));
        let addr = ctx.addr.clone();
        self.subscriber_pc.on_ice_candidate(Box::new(move |candidate| {
            let addr = addr.clone();
            Box::pin(async move {
                if let Some(cand) = candidate {
                    if let Ok(RTCIceCandidateInit { candidate, sdp_mid, sdp_mline_index, .. }) = cand.to_json() {
                        let _ = addr.send(UserMessage::IceCandidate { candidate: IceCandidate { candidate, sdp_mid, sdp_mline_index }, target: Target::Subscriber }).await;
                    }
                }
            })
        }));
        let addr = ctx.addr.clone();
        let pc = Arc::clone(&self.subscriber_pc);
        self.subscriber_pc.on_negotiation_needed(Box::new(move || {
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
                        let _ = addr.send(UserMessage::Signal {
                            offer: offer.sdp
                        }).await;
                    }
                    Err(e) => tracing::error!("[User] create_offer: {:?}", e),
                }
            })
        }));
        let room_addr = self.room.clone();
        let peer_id = self.peer_id.clone();
        let publisher_pc = self.publisher_pc.clone();
        self.publisher_pc.on_track(Box::new(move |track, _, _| {
            let room = room_addr.clone();
            let publisher_pc = publisher_pc.clone();
            let ssrc = track.ssrc();
            let mime_type = track.codec().capability.mime_type;
            let kind = track.kind();
            let rid = StreamQuality::from_str(track.rid());
            
            Box::pin(async move {
                tracing::info!("[SFU] {kind} от {peer_id} подключен");
                let (tx, stream) = tokio::sync::broadcast::channel(16);
                let active_receiver_counter =  Arc::new(AtomicUsize::new(0));
                let stream = Arc::new(stream);
                let mesage = match kind {
                        RTPCodecType::Audio | RTPCodecType::Unspecified => { 
                            let packet_subscription = PacketAudioSubscription { 
                                active_receiver_counter: active_receiver_counter.clone(), 
                                stream, 
                            };
                            RoomMessage::AddAudioTrack { 
                            stream: PeerStream {
                                mime_type,
                                packet_subscription,
                            }, 
                            peer_id 
                        }
                    },
                    RTPCodecType::Video => {
                        let pli_sender = PliSender::new(publisher_pc, ssrc).start();
                        let packet_subscription = PacketVideoSubscription { 
                            active_receiver_counter: active_receiver_counter.clone(), 
                            pli_sender,
                            stream, 
                        };
                        let quality = rid.unwrap_or(StreamQuality::High);
                        RoomMessage::AddVideoTrack { 
                            stream: PeerStream {
                                mime_type,
                                packet_subscription,
                            }, 
                            peer_id,
                            quality
                        }
                    }   
                };
                let _ = room.send(mesage).await;
                tokio::spawn(async move {
                    if let Err(e) = forward_rtp_packets(&track, &tx, &active_receiver_counter).await {
                        tracing::error!("[SFU] forward_rtp_packets failed {e}");
                    }
                });
            })
        }));
        tracing::info!("🟢 [Userctor] Пользователь инициализирован.");
    }
    async fn stopping(&mut self, _: &Ctx<'_, Self>) {
        if let Err(e) = self.publisher_pc.close().await {
            tracing::error!("[UserActor] publisher_pc.close() {e}");
        }
        if let Err(e) = self.subscriber_pc.close().await {
            tracing::error!("[UserActor] publisher_pc.close() {e}");
        }
        let _ = self.room.send(RoomMessage::Leave { peer_id: self.peer_id.clone() }).await;
        tracing::info!("🔴 [UserActor] Пользователь уничтожен.");
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SignalMessage {
    target: Target,
    #[serde(flatten)] 
    message_type: MessageType,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")] 
pub enum MessageType {
    Offer { sdp: String },
    Answer { sdp: String },
    Candidate { 
        #[serde(flatten)]
        candidate: IceCandidate 
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct IceCandidate {
    candidate: String, 
    sdp_mid: Option<String>, 
    sdp_mline_index: Option<u16>,
}


#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum Target {
    Publisher,
    Subscriber
}

impl User {
    async fn send_welcome(&mut self) -> Result<(), Error> {
        let welcome_payload = serde_json::json!({
            "type": "welcome",
            "assigned_peer_id": self.peer_id
        });
        self.ws_tx.send(serde_json::to_string(&welcome_payload)?.into()).await?;
        Ok(())
    }
    async fn handle_ws_message(&mut self, message: Result<Message, Error>) -> Result<(), Error> {
        let text = match message? {
            Message::Close(_) => {
                let _ = self.room.send(RoomMessage::Leave { peer_id: self.peer_id.clone() }).await;
                return Ok(());
            },
            Message::Text(text) => text,
            _ => return Ok(())
        };
        let sdp: SignalMessage = serde_json::from_str(&text).map_err(|e| {
            println!("{text}");
            e
        })?;
        match (sdp.target, sdp.message_type) {
            (Target::Publisher, MessageType::Offer {sdp}) => {
                let offer_desc = RTCSessionDescription::offer(sdp)?;
                self.publisher_pc.set_remote_description(offer_desc).await?;
                let answer = self.publisher_pc.create_answer(None).await?;
                 let mut gather_complete = self.publisher_pc.gathering_complete_promise().await;
                self.publisher_pc.set_local_description(answer.clone()).await?;
                let _ = gather_complete.recv().await; //
                let message = SignalMessage {
                    target: Target::Publisher,
                    message_type: MessageType::Answer {sdp: answer.sdp }
                };
                let json_answer = serde_json::to_string(&message)?;
                self.ws_tx.send(Message::Text(json_answer.into())).await?;
                let _ = self.room.send(RoomMessage::SubscribeToPeers { peer_id: self.peer_id }).await;
            },
            (Target::Subscriber, MessageType::Answer { sdp }) => {
                let answer_desc = RTCSessionDescription::answer(sdp)?;
                self.subscriber_pc.set_remote_description(answer_desc).await?;
            },
            (Target::Publisher, MessageType::Candidate { candidate }) => {
                let IceCandidate { candidate, sdp_mid, sdp_mline_index } = candidate;
                let candidate_init = RTCIceCandidateInit {
                    candidate,
                    sdp_mid,
                    sdp_mline_index,
                    ..Default::default()
                };
                self.publisher_pc.add_ice_candidate(candidate_init).await?;
            },
            (Target::Subscriber, MessageType::Candidate { candidate }) => {
                let IceCandidate { candidate, sdp_mid, sdp_mline_index } = candidate;
                let candidate_init = RTCIceCandidateInit {
                    candidate,
                    sdp_mid,
                    sdp_mline_index,
                    ..Default::default()
                };
                self.subscriber_pc.add_ice_candidate(candidate_init).await?;
            },
            (target, r#type) => {
                println!("{:?} {:?}",  target, r#type);
            }
        }
        Ok(())
    }
    async fn connect_audio(&mut self, user: Addr<Self>, request: ConnectionRequest<PacketAudioSubscription>) -> Result<(), Error> {
        let peer_id = request.speaker_id;
        let audio_subscription = AudioSubscription::init(self.subscriber_pc.clone(), user, request).await?;
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
                    self.subscriber_pc.clone(), 
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
    async fn send_offer(&mut self, sdp: String) -> Result<(), Error> {
        let signaling_message = serde_json::to_string(&SignalMessage {
            target: Target::Subscriber,
            message_type: MessageType::Offer { sdp }
        })?;
        self.ws_tx.send(Message::Text(signaling_message.into())).await?;
        Ok(())
    }
    async fn send_ice_candidate(&mut self, target: Target, candidate: IceCandidate) -> Result<(), Error> {
        let signaling_message = SignalMessage {
            target,
            message_type: MessageType::Candidate { candidate }
        };
        let str = serde_json::to_string(&signaling_message)?;
        self.ws_tx.send(Message::Text(str.into())).await?;
        Ok(())
    }
    async fn disconnect_from_user(&mut self, speaker_id: Uuid) -> Result<(), Error> {
        // let subscription = self.subscriptions
        //     .remove(&speaker_id)
        //     .ok_or(Error::SystemError { message: "subscription not found".into() })?;
        // subscription.disconnect(&self.peer_connection).await?;
        
        let notice = serde_json::json!({
            "type": "peer_left",
            "peer_id": speaker_id
        });
        self.ws_tx.send(Message::Text(notice.to_string().into())).await?;
        Ok(())
    }
}