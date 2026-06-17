use std::{collections::HashMap, str::FromStr, sync::Arc};
use uuid::Uuid;
use webrtc::{ice_transport::{ice_candidate::RTCIceCandidateInit, ice_connection_state::RTCIceConnectionState}, peer_connection::{RTCPeerConnection, sdp::session_description::RTCSessionDescription}, rtp_transceiver::rtp_codec::RTPCodecType};
use crate::{PacketAudioSubscription, PacketVideoSubscription, SyncChannel, actor::{Actor, Addr, Ctx, WeakAddr}, audio_packet_forwarder::AudioPacketForwarder, create_peer, error::Error, pli_sender::{Ping, PliSender}, quality_monitor::{DeviceType, QualityMonitor, QualityThresholds}, room::{MimeType, Room, RoomMessage, StreamQuality}, rtp_packet_forwarder::RtpPacketForwarder, user::{IceCandidate, MessageType, SignalMessage, Target, User, UserMessage, initiate_ice_restart}, video_packet_forwarder::VideoPacketForwarder};

pub struct Publisher<S: SyncChannel> {
    pub peer_id: Uuid,
    pub pc: Arc<RTCPeerConnection>,
    pub video_tracks: HashMap<StreamQuality, VideoTrack>,
    pub audio_track: Option<AudioTrack>,
    pub user: Addr<User<S>>,
    pub room: Addr<Room<S>>,    
    pub qualify_monitor: WeakAddr<QualityMonitor<S>>,
    pub disconnected: bool,
    pub retry_connect_attempts: u8,
}

#[derive(Clone)]
pub struct AudioTrack {
    mime_type: MimeType,
    addr: Addr<RtpPacketForwarder<AudioPacketForwarder>>
}

#[derive(Clone)]
pub struct VideoTrack {
    mime_type: MimeType,
    pli_sender: Addr<PliSender>,
    addr: Addr<RtpPacketForwarder<VideoPacketForwarder>>
}

const TARGET: Target = Target::Publisher;

impl<S: SyncChannel> Publisher<S> {
    pub async fn new(user: Addr<User<S>>, room: Addr<Room<S>>, peer_id: Uuid) -> Result<Self, Error> {
        let peer = create_peer().await?;
        let pc = Arc::new(peer);
        Ok(Self {
            pc,
            user,
            room,
            peer_id,
            video_tracks: HashMap::with_capacity(3),
            audio_track: None,
            qualify_monitor: WeakAddr::default(),
            disconnected: false,
            retry_connect_attempts: 0
        })
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
    NewAudioTrack(AudioTrack),
    NewVideoTrack{ quality: StreamQuality, track: VideoTrack },
}

impl<S: SyncChannel> Actor for Publisher<S> {
    type Message = PublisherMessage;
    async fn handle(&mut self, ctx: &mut Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            PublisherMessage::CheckIceState => {
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
                            let _ = addr.send(PublisherMessage::CheckIceState).await;
                        });
                    } else {
                        self.stop(ctx).await;
                    }
                }
            }
            PublisherMessage::NewAudioTrack(track) if self.audio_track.is_none() => {
                self.audio_track = Some(track.clone());
                let _ = self.room.send(RoomMessage::AddAudioTrack { 
                        peer_id: self.peer_id, 
                        stream: crate::room::PeerStream { packet_subscription: PacketAudioSubscription {
                            rtp_packet_forwarder: track.addr,
                        }, mime_type: track.mime_type 
                    }, 
                }).await;
            }
            PublisherMessage::NewAudioTrack(track) =>  {
                todo!("implement NewAudioTrack resubscribe")
            }
            PublisherMessage::NewVideoTrack {quality, track} => {
                match self.video_tracks.entry(quality) {
                    std::collections::hash_map::Entry::Occupied(o) => {
                        todo!("implement NewAudioTrack resubscribe");
                    },
                    std::collections::hash_map::Entry::Vacant(v) => {
                        v.insert(track.clone());
                        let _ = self.room.send(RoomMessage::AddVideoTrack { 
                            peer_id: self.peer_id, 
                            quality,
                            stream: crate::room::PeerStream { 
                                packet_subscription: PacketVideoSubscription {
                                    rtp_packet_forwarder: track.addr,
                                    pli_sender: track.pli_sender
                                }, 
                                mime_type: track.mime_type 
                            }, 
                        }).await;
                    }
                }
            },
            PublisherMessage::Websocket(message) => {
                if let Err(e) = self.handle_ws_message(message).await {
                    tracing::error!("{e}");
                }
            }
            PublisherMessage::IceStateChange { state } => {
                if let Err(e) = self.handle_ice_state_change(state).await {
                    tracing::error!("[UserActor] UserMessage::Signal {e}");
                }
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
        let addr = ctx.addr.clone();
        let pc = self.pc.clone();
        self.pc.on_track(Box::new(move |track, _, _| {
            let pc = pc.clone();
            let addr = addr.clone();
            let ssrc = track.ssrc();
            let mime_type = track.codec().capability.mime_type;
            let mime_type = MimeType::from_str(&mime_type).map_err(|e| {
                tracing::error!("{e}");
                e
            }).unwrap_or_default();
            let kind = track.kind();
            let rid = StreamQuality::from_str(track.rid());
            Box::pin(async move {
                match kind {
                    RTPCodecType::Audio | RTPCodecType::Unspecified => { 
                        let rtp_packet_forwarder= RtpPacketForwarder::<AudioPacketForwarder>::spawn(track, StreamQuality::Audio);
                        let _ = addr.send(
                            PublisherMessage::NewAudioTrack(AudioTrack { mime_type, addr: rtp_packet_forwarder })
                        ).await;
                    },
                    RTPCodecType::Video => {
                        let pli_sender = PliSender::new(pc.clone(), ssrc).start();
                        let quality = rid.unwrap_or(StreamQuality::High);
                        let rtp_packet_forwarder= RtpPacketForwarder::<VideoPacketForwarder>::spawn(track, quality);
                        let _ = addr.send(
                            PublisherMessage::NewVideoTrack {
                                track: VideoTrack { mime_type, pli_sender, addr: rtp_packet_forwarder } , 
                                quality 
                            }
                        ).await;
                    }   
                };
            })
        }));
    }
    async fn stopping(self, _: &Ctx<'_, Self>) {
        self.qualify_monitor.strong().terminate().await;
        if let Err(e) = self.pc.close().await {
            tracing::error!("[Publisher] stopping {e}");
        }
        tracing::info!("🔴 [Publisher] Пользователь уничтожен.");
    }
}

impl<S: SyncChannel> Publisher<S> {
    async fn handle_ice_state_change(&mut self, state: RTCIceConnectionState) -> Result<(), Error> {
        match state {
            RTCIceConnectionState::Disconnected | RTCIceConnectionState::Failed => {
                tracing::warn!("[Publisher] IceStateChange");
                self.disconnected = true;
                let message = initiate_ice_restart(&self.pc, Target::Publisher).await?;
                let _ = self.user.send(UserMessage::SignalMessage(message)).await;
                self.retry_connect_attempts += 1;
            },
            RTCIceConnectionState::Connected if self.disconnected => {
                self.disconnected = false;
                self.retry_connect_attempts = 0;
                for VideoTrack { pli_sender, .. } in self.video_tracks.values() {
                    let _ = pli_sender.send(Ping).await;
                }
            },
            _ => {}
        }
        Ok(())
    }
    
    async fn handle_ws_message(&mut self, message: MessageType) -> Result<(), Error> {
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
            MessageType::Answer { sdp } => {
                let answer_desc = RTCSessionDescription::answer(sdp)?;
                self.pc.set_remote_description(answer_desc).await?;
            },
            _ => {}
        }
        Ok(())
    }
}