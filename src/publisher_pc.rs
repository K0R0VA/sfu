use std::{str::FromStr, sync::{Arc}};
use uuid::Uuid;
use webrtc::{api::{APIBuilder, interceptor_registry::register_default_interceptors, media_engine::MediaEngine, setting_engine::SettingEngine}, ice_transport::{ice_candidate::RTCIceCandidateInit, ice_connection_state::RTCIceConnectionState, ice_server::RTCIceServer}, interceptor::registry::Registry, peer_connection::{RTCPeerConnection, configuration::RTCConfiguration, offer_answer_options::RTCOfferOptions, sdp::session_description::RTCSessionDescription}, rtp_transceiver::rtp_codec::{RTCRtpHeaderExtensionCapability, RTPCodecType}};
use crate::{PacketAudioSubscription, PacketVideoSubscription, actor::{Actor, Addr, Ctx, WeakAddr}, audio_packet_forwarder::AudioPacketForwarder, error::Error, pli_sender::PliSender, quality_monitor::{DeviceType, QualityMonitor, QualityThresholds}, room::{MimeType, PeerStream, Room, RoomMessage, StreamQuality}, rtp_packet_forwarder::RtpPacketForwarder, user::{IceCandidate, MessageType, SignalMessage, Target, User, UserMessage}, video_packet_forwarder::VideoPacketForwarder};

pub struct Publisher  {
    pub user: Addr<User>,
    pub room: Addr<Room>,
    pub peer_id: Uuid,
    pub pc: Arc<RTCPeerConnection>,
    pub qualify_monitor: WeakAddr<QualityMonitor>,
}

const TARGET: Target = Target::Publisher;

impl Publisher {
    pub async fn new(user: Addr<User>, room: Addr<Room>, peer_id: Uuid) -> Result<Self, Error> {
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
            pc,
            user,
            room,
            peer_id,
            qualify_monitor: WeakAddr::default()
        })
    }
}

pub enum PublisherMessage {
    Websocket (MessageType),
    IceStateChange {
        state: RTCIceConnectionState,
    },
    IceCandidate { 
        candidate: IceCandidate,
    },
    InitiateMonitoring { device_type: DeviceType }
}

impl Actor for Publisher {
    type Message = PublisherMessage;
    async fn handle(&mut self, ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            PublisherMessage::Websocket(message) => {
                if let Err(e) = self.handle_ws_message(ctx, message).await {
                    tracing::error!("{e}");
                }
            }
            PublisherMessage::IceStateChange { state } => {
                // if let Err(e) = self.handle_ice_state_change(state, target).await {
                //     tracing::error!("[UserActor] UserMessage::Signal {e}");
                // }
            }
            PublisherMessage::IceCandidate { candidate } => {
               let message = SignalMessage::Rtc {
                    target: TARGET,
                    message_type: MessageType::Candidate { candidate }
                };
                let _ = self.user.send(UserMessage::SignalMessage(message)).await;
            },
            PublisherMessage::InitiateMonitoring { device_type } => {
                let quality_monitor = QualityMonitor::new(self.pc.clone(), self.user.clone(), QualityThresholds::from(device_type)).start();
                self.qualify_monitor.set_addr(quality_monitor);
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
        self.pc.on_ice_connection_state_change(Box::new({
            let addr = addr.clone();
            move |state| {
                let addr = addr.clone();
                Box::pin(async move {
                    let _ = addr.send(PublisherMessage::IceStateChange { state }).await;
                })
            }
        }));
        let room = self.room.clone();
        let peer_id = self.peer_id.clone();
        let publisher_pc = self.pc.clone();
        self.pc.on_track(Box::new(move |track, _, _| {
            let room = room.clone();
            let publisher_pc = publisher_pc.clone();
            let ssrc = track.ssrc();
            let mime_type = track.codec().capability.mime_type;
            let mime_type = MimeType::from_str(&mime_type).map_err(|e| {
                tracing::error!("{e}");
                e
            }).unwrap_or_default();
            let kind = track.kind();
            let rid = StreamQuality::from_str(track.rid());
            
            Box::pin(async move {
                tracing::info!("[SFU] {kind} от {peer_id} подключен");
                let mesage = match kind {
                        RTPCodecType::Audio | RTPCodecType::Unspecified => { 
                            let rtp_packet_forwarder= RtpPacketForwarder::<AudioPacketForwarder>::spawn(track, StreamQuality::Audio);
                            let packet_subscription = PacketAudioSubscription { 
                                rtp_packet_forwarder
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
                        let quality = rid.unwrap_or(StreamQuality::High);
                        let pli_sender = PliSender::new(publisher_pc, ssrc).start();
                        let rtp_packet_forwarder= RtpPacketForwarder::<VideoPacketForwarder>::spawn(track, quality);
                        let packet_subscription = PacketVideoSubscription { 
                            rtp_packet_forwarder,
                            pli_sender,
                        };
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
            })
        }));
        tracing::info!("🟢 [Userctor] Пользователь инициализирован.");
    }
    async fn stopping(self, _: &Ctx<'_, Self>) {
        self.qualify_monitor.strong().terminate().await;
        if let Err(e) = self.pc.close().await {
            tracing::error!("[Publisher] stopping {e}");
        }
        tracing::info!("🔴 [Publisher] Пользователь уничтожен.");
    }
}

impl Publisher {
    async fn handle_ice_state_change(&mut self, ctx: &mut Ctx<'_, Self>, state: RTCIceConnectionState, target: Target) -> Result<(), Error> {
        match (state, target) {
            (RTCIceConnectionState::Failed, target) => {
                // initiate_ice_restart(target).await?;
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
    
    async fn handle_ws_message(&mut self, _ctx: &mut Ctx<'_, Self>, message: MessageType) -> Result<(), Error> {
        match message {
            MessageType::Offer {sdp} => {
                let offer_desc = RTCSessionDescription::offer(sdp)?;
                self.pc.set_remote_description(offer_desc).await?;
                let answer = self.pc.create_answer(None).await?;
                 let mut gather_complete = self.pc.gathering_complete_promise().await;
                self.pc.set_local_description(answer.clone()).await?;
                let _ = gather_complete.recv().await; //
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
            _ => {}
        }
        Ok(())
    }
}