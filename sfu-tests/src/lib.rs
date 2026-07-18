use std::{collections::HashSet, fs::File, io::BufReader, str::FromStr, sync::{Arc}, time::Duration};

use sfu::{Storage, StorageConfiguration, SyncChannel, create_peer, error::Error, user::{IceCandidate, SignalMessage, Target}};
use tokio::{io::AsyncWriteExt, sync::Mutex};
use uuid::Uuid;
use webrtc::{ice_transport::ice_candidate::RTCIceCandidateInit, media::{Sample, io::ivf_reader::{ IVFReader}}, peer_connection::{RTCPeerConnection, sdp::session_description::RTCSessionDescription}, rtp_transceiver::{RTCRtpTransceiverInit, rtp_codec::RTCRtpCodecCapability, rtp_transceiver_direction::RTCRtpTransceiverDirection}, track::track_local::track_local_static_sample::TrackLocalStaticSample
};

#[cfg(test)]
mod connect_to_room;
#[cfg(test)]
mod ice_restart;

pub struct TestClient {
    pub peer_id: Option<Uuid>,
    pub publisher_pc: Arc<RTCPeerConnection>,
    pub subscriber_pc: Arc<RTCPeerConnection>,
    pub sfu_tx: tokio::sync::mpsc::Sender<SignalMessage>,
    pub sfu_rx: tokio::sync::mpsc::Receiver<SignalMessage>,
    pub connected_peers: Arc<Mutex<HashSet<Uuid>>>,
    pub markers: Markers,
}

pub struct Markers {
    pub ice_connected: bool,
    pub send_offer: bool,
    pub receive_answer: bool,
    pub receive_offer: bool,
}

impl TestClient {
    pub async fn new(sfu_tx: tokio::sync::mpsc::Sender<SignalMessage>, sfu_rx: tokio::sync::mpsc::Receiver<SignalMessage>,) -> Result<Self, Error> {
        let publisher_pc = Arc::new(create_peer().await?);
        let subscriber_pc = Arc::new(create_peer().await?);
        Ok(Self {
            peer_id: None,
            publisher_pc,
            subscriber_pc,
            sfu_rx,
            sfu_tx,
            connected_peers: Arc::new(Mutex::new(HashSet::new())),
            markers: Markers { ice_connected: false, receive_answer: false, send_offer: false, receive_offer: false, },
        })
    }
    pub async fn setup(&self) -> Result<(), sfu::error::Error> {
        let channel = self.sfu_tx.clone();
        self.publisher_pc.on_ice_candidate(Box::new(move |c| {
            if let Some(candidate) = c.and_then(|c| c.to_json().ok()) {
                let candidate = IceCandidate {
                    candidate: candidate.candidate,
                    sdp_mid: candidate.sdp_mid,
                    sdp_mline_index: candidate.sdp_mline_index
                };
                let channel = channel.clone();
                tokio::spawn(async move {
                    let msg = SignalMessage::Rtc {
                        target: sfu::user::Target::Publisher,
                        message_type: sfu::user::MessageType::Candidate { candidate  }
                    };
                    let _ = channel.send(msg).await;
                });
            }
            Box::pin(async {})
        }));
        let channel = self.sfu_tx.clone();
        self.subscriber_pc.on_ice_candidate(Box::new(move |c| {
            if let Some(candidate) = c.and_then(|c| c.to_json().ok()) {
                let candidate = IceCandidate {
                    candidate: candidate.candidate,
                    sdp_mid: candidate.sdp_mid,
                    sdp_mline_index: candidate.sdp_mline_index
                };
                let channel = channel.clone();
                tokio::spawn(async move {
                    let msg = SignalMessage::Rtc {
                        target: sfu::user::Target::Subscriber,
                        message_type: sfu::user::MessageType::Candidate { candidate  }
                    };
                    let _ = channel.send(msg).await;
                });
            }
            Box::pin(async {})
        }));
        let connected_peers = Arc::clone(&self.connected_peers);
        self.subscriber_pc.on_track(Box::new(move |track, _, _| {
            let peer_id = track.stream_id();
            let peers = connected_peers.clone();
            println!("📥 [Тест] Успешно получен медиа-трек от SFU: {}", track.id());
            Box::pin(async move {
                peers.lock().await.insert(Uuid::from_str(&peer_id).unwrap());
                tokio::spawn(async move {
                    loop {
                        let _ = track.read_rtp().await?;
                    }
                    #[allow(unused)]
                    Result::<_, sfu::error::Error>::Ok(())
                });
            })
        }));
        Ok(())
    }
    async fn send_track(&mut self, peer_id: Uuid) -> Result<(), Error> {
        self.peer_id = Some(peer_id);
        self.add_track().await?;
        self.markers.send_offer = true;
        self.sfu_tx.send(SignalMessage::Connect { device_type: sfu::quality_monitor::DeviceType::Desktop }).await.unwrap();
        Ok(())
    }
    async fn handle_offer(&self, sdp: String) -> Result<(), Error> {
        let description = RTCSessionDescription::offer(sdp)?;
        self.subscriber_pc.set_remote_description(description).await?;
        let answer = self.subscriber_pc.create_answer(None).await?;
        self.subscriber_pc.set_local_description(answer.clone()).await?;
        let response = SignalMessage::Rtc { 
            target: sfu::user::Target::Subscriber, 
            message_type: sfu::user::MessageType::Answer { sdp: answer.sdp }
        };
        self.sfu_tx.send(response).await.unwrap();
        Ok(())
    }
    async fn handle_ice_restart(&self, target: Target, pc: &RTCPeerConnection, sdp: String) -> Result<(), Error> {
        let description = RTCSessionDescription::offer(sdp)?;
        pc.set_remote_description(description).await?;
        let answer = pc.create_answer(None).await?;
        pc.set_local_description(answer.clone()).await?;
        let response = SignalMessage::Rtc { 
            target, 
            message_type: sfu::user::MessageType::Answer { sdp: answer.sdp }
        };
        self.sfu_tx.send(response).await.unwrap();
        Ok(())
    }
    async fn handle_ice_candidate(&self, pc: &RTCPeerConnection, IceCandidate { candidate, sdp_mid, sdp_mline_index }: IceCandidate) -> Result<(), Error> {
        pc.add_ice_candidate(RTCIceCandidateInit {
            candidate,
            sdp_mid,
            sdp_mline_index, 
            ..Default::default()
        }).await?;
        Ok(())
    }
    pub async fn handle_message(&mut self) -> Result<(), Error> {
        if let Some(message) = self.sfu_rx.recv().await {
            match message {
                SignalMessage::Welcome { peer_id } => self.send_track(peer_id).await?,
                SignalMessage::PeerLeft { peer_id }  => {
                    let mut peers = self.connected_peers.lock().await;
                    peers.remove(&peer_id);
                }
                SignalMessage::Rtc { target, message_type } => {
                    let pc = match target {
                        sfu::user::Target::Publisher => &self.publisher_pc,
                        sfu::user::Target::Subscriber => &self.subscriber_pc
                    };
                    match message_type {
                        sfu::user::MessageType::Candidate { candidate } => {
                            self.handle_ice_candidate(pc, candidate).await?;
                            self.markers.ice_connected = true;
                        }
                        sfu::user::MessageType::Answer { sdp } => {
                            let description = RTCSessionDescription::answer(sdp)?;
                            self.publisher_pc.set_remote_description(description).await?;
                            self.markers.receive_answer = true;
                        }
                        sfu::user::MessageType::Offer { sdp } => {
                            self.handle_offer(sdp).await?;
                            self.markers.receive_offer = true;
                        },
                        sfu::user::MessageType::IceRestart { sdp } => self.handle_ice_restart(target, pc, sdp).await?
                    }
                },
                _ => {}
            }
        }
        Ok(())
    }
    async fn add_track(&mut self) -> Result<(), Error> {
        let video_track: Arc<TrackLocalStaticSample> = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "video/VP8".to_string(),
                ..Default::default()
            },
            "video".to_string(),
            self.peer_id.unwrap().to_string()
        ));

        self.publisher_pc.add_transceiver_from_track(
            Arc::clone(&video_track) as Arc<_>,
            Some(RTCRtpTransceiverInit { 
                direction: RTCRtpTransceiverDirection::Sendonly, 
                send_encodings: vec![] 
            })
        ).await?;
        
        let offer = self.publisher_pc
            .create_offer(None)
            .await?;
        self.publisher_pc.set_local_description(offer.clone()).await?;
        self.sfu_tx.send(SignalMessage::Rtc { 
            target: sfu::user::Target::Publisher, 
            message_type: sfu::user::MessageType::Offer { 
                sdp: offer.sdp
            } 
        }).await.unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(32);
        tokio::task::spawn_blocking(move || {
            let file = File::open("../output.ivf").unwrap();
            let buf_reader = BufReader::new(file);
            let (mut reader, header) = IVFReader::new(buf_reader).unwrap();
            let sleep_time = Duration::from_millis(
                ((1000 * header.timebase_numerator) / header.timebase_denominator) as u64,
            );
            loop {
                let frame = match reader.parse_next_frame() {
                    Ok((frame, _)) => frame,
                    Err(_) => {
                        let file = File::open("../output.ivf").unwrap();
                        let buf_reader = BufReader::new(file);
                        reader = IVFReader::new(buf_reader).unwrap().0;
                        continue;
                    }
                };
                if tx.blocking_send((frame, sleep_time)).is_err() {
                    break ;
                }
                std::thread::sleep(sleep_time);
            }
        });
        tokio::task::spawn(async move {
            while let Some((frame, duration)) = rx.recv().await {
                video_track
                    .sample_writer()
                    .write_sample(&Sample {
                        data: frame.freeze(),
                        duration,
                        ..Default::default()
                    })
                    .await?;
            }
            Result::<_, Error>::Ok(())
        });
        Ok(())
    }
}

pub async fn spawn_test_client() -> Result<(TestSyncChannel, TestClient, tokio::sync::mpsc::Receiver<SignalMessage>), sfu::error::Error> {
    let (client_tx, server_rx) = tokio::sync::mpsc::channel(32);
    let (server_tx, client_rx) = tokio::sync::mpsc::channel(32);
    let client = TestClient::new(client_tx, client_rx).await?;
    let channel = TestSyncChannel {channel: server_tx};
    Ok((channel, client, server_rx))
}

pub struct TestSyncChannel {
    channel: tokio::sync::mpsc::Sender<SignalMessage>                               
}

impl SyncChannel for TestSyncChannel {
    type Item = SignalMessage;
    type Error = tokio::sync::mpsc::error::SendError<SignalMessage>;
    async fn send(&mut self, message: SignalMessage) -> Result<(), Self::Error> {
        self.channel.send(message)
            .await?;
        Ok(())
    }
}

pub struct FileStorage {
    file: tokio::fs::File
}

pub struct FileConfiguration { path: String }

impl StorageConfiguration for FileConfiguration {
    type Error = std::env::VarError;
    fn from_env() -> Result<Self, Self::Error> {
        let output_dir = std::env::var("output_dir")?;
        let file_name = format!("{}.txt", Uuid::new_v4());
        let path = format!("{output_dir}/{file_name}");
        Ok(Self {
            path
        })
    }
}
 
impl Storage for FileStorage {
    type Configuration = FileConfiguration;
    type Error = std::io::Error;
    async fn connect(configuration: &Self::Configuration) -> Result<Self, Self::Error> {
        let file = tokio::fs::File::create_new(&configuration.path).await?;
        Ok(Self {
            file
        })
    }
    async fn insert(&mut self, item: sfu::StorageItem<'_>) -> Result<(), Self::Error> {
        let raw = format!("[{} {}] {:?}", item.connection_id, item.timestamp, item.stats, );
        self.file.write_all(raw.as_bytes()).await?;
        Ok(())
    }
}