use std::{collections::HashMap, sync::Arc};
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, stream::SplitSink};
use webrtc::{peer_connection::{RTCPeerConnection, sdp::session_description::RTCSessionDescription}, rtp::packet::Packet, rtp_transceiver::{RTCRtpTransceiverInit, rtp_transceiver_direction::RTCRtpTransceiverDirection}, track::track_local::{TrackLocalWriter, track_local_static_rtp::TrackLocalStaticRTP}};
use crate::{actor::{Actor, Addr, Ctx}, error::Error, room::{Peer, Room, RoomMessage}};

pub struct User {
    pub room: Addr<Room>,
    pub joined_room: bool,
    pub peer_id: String,
    pub ws_tx: SplitSink<WebSocket, Message>,
    pub peer_connection: Arc<RTCPeerConnection>,
    pub local_track: Arc<TrackLocalStaticRTP>,
    pub subscriptions: HashMap<String, Arc<TrackLocalStaticRTP>>
}

pub enum UserMessage {
    Websocket {
        message: Result<Message, Error>
    },
    ConnectToUser {
        speaker_id: String,
        track: Arc<TrackLocalStaticRTP>
    },
    Broadcast {
        speaker_id: String,
        stream: tokio::sync::broadcast::Receiver<Packet>,
    }
}

impl Actor for User {
    type Message = UserMessage;
    async fn handle(&mut self, ctx: &Ctx<'_, Self>, msg: Self::Message) {
        match msg {
            UserMessage::Websocket { message } => {
                if let Err(e) = self.handle_ws_message(ctx, message).await {
                    println!("handle_ws_message failed {:?}", e);
                    if self.joined_room {
                        let _ = self.room.send(RoomMessage::Leave { peer_id: self.peer_id.clone() }).await;
                    }
                }
            },
            UserMessage::ConnectToUser { speaker_id, track } => {
                if let Err(e) = self.connect_to_user(track, &speaker_id).await {
                    println!("handle_ws_message failed {:?}", e);
                    let _ = self.room.send(RoomMessage::Leave { peer_id: self.peer_id.clone() }).await;
                }
            }
            UserMessage::Broadcast { mut stream,speaker_id } => {
                let local_track = self.local_track.clone();
                let peer_id = self.peer_id.clone();
                tokio::spawn(async move {
                    let mut packet_count = 0;
                    while let Ok(packet) = stream.recv().await {
                        packet_count += 1;
                        if packet_count % 100 == 0 {
                            println!(
                                "📤 [МЕДИА-ВЫХОД] Актор {} пересылает 100-й пакет соседа в сеть. Seq={}", 
                                peer_id, packet.header.sequence_number
                            );
                        }
                        if let Err(e) = local_track.write_rtp(&packet).await {
                            tracing::error!("received error from write_rtp {e}");
                        }
                    }
                });
            }
        }
    }   
    async fn starting(&mut self, _: &Ctx<'_, Self>) {
        println!("🟢 [Userctor] Пользователь инициализирован.");
    }
    async fn stop(&mut self) {
        println!("🔴 [UserActor] Пользователь уничтожен.");
    }
}

impl User {
    async fn handle_ws_message(&mut self, ctx: &Ctx<'_, Self>, message: Result<Message, Error>) -> Result<(), Error> {
        let text = match message? {
            Message::Close(_) => {
                let _ = self.room.send(RoomMessage::Leave { peer_id: self.peer_id.clone() }).await;
                return Ok(());
            },
            Message::Text(text) => text,
            _ => return Ok(())
        };
        let sdp: RTCSessionDescription = serde_json::from_str(&text)?;
        self.peer_connection.set_remote_description(sdp).await?;
        if !self.joined_room {
            let _ = self.room.send(RoomMessage::Join { peer_id: self.peer_id.clone(), peer: Peer {
                user: ctx.addr.clone(),
                track: self.local_track.clone()
            } }).await;
            self.joined_room = true;
        }
        let answer = self.peer_connection.create_answer(None).await?;
        let mut gather_complete = self.peer_connection.gathering_complete_promise().await;
        self.peer_connection.set_local_description(answer).await?;
        let _ = gather_complete.recv().await; // Ждем сбора всех ICE-кандидатов
        if let Some(local_desc) = self.peer_connection.local_description().await {
            let json_answer = serde_json::to_string(&local_desc)?;
            self.ws_tx.send(Message::Text(json_answer.into())).await?;
            
        }
        Ok(())
    }
    async fn connect_to_user(&mut self, track: Arc<TrackLocalStaticRTP>, speaker_id: &str) -> Result<(), Error> {
        self.peer_connection.add_transceiver_from_track(
                track as Arc<_>,
                Some(RTCRtpTransceiverInit { direction: RTCRtpTransceiverDirection::Sendonly, send_encodings: vec![] })
            ).await?;
        let notice = serde_json::json!({
            "type": "peer_joined",
            "peer_id": speaker_id
        });
        self.ws_tx.send(Message::Text(notice.to_string().into())).await?;
        Ok(())
    }
}
