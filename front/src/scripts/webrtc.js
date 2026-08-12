import { getDeviceType } from "./websocket";

export class WebrtcConnection {
    constructor(users, ws) {
        this.subscriber = new SubscriberConnection(ws, users);
        this.publisher = new PublisherConnection(ws);
    }
    async add_ice_candidate(message) {
        switch (message.target) {
            case 'publisher': {
                await this.publisher.add_ice_candidate(message);
                break;
            }
            case 'subscriber': {
                await this.subscriber.add_ice_candidate(message);
                break;
            }
        }
    }
    async receive_answer(message) {
        switch (message.target) {
            case 'publisher': {
                await this.publisher.receive_answer(message);
                break;
            }
            case 'subscriber': {
                await this.subscriber.receive_answer(message);
                break;
            }
        }
    }
}

const BASE_WIDTH = 1080;
const BASE_HEIGHT = 1920;

function getRoundedResolution(baseWidth, baseHeight, targetScale, alignment = 32) {
    if (targetScale <= 1.0) {
        // Для high слоя - никаких округлений
        return 1.0;
    }
    
    let targetWidth = baseWidth / targetScale;
    let targetHeight = baseHeight / targetScale;
    
    let roundedWidth = Math.round(targetWidth / alignment) * alignment;
    let roundedHeight = Math.round(targetHeight / alignment) * alignment;
    
    // Убеждаемся, что не увеличиваем разрешение
    if (roundedWidth > baseWidth || roundedHeight > baseHeight) {
        return targetScale; // Возвращаем исходный коэффициент
    }
    
    let realScaleDown = baseWidth / roundedWidth;
    
    // Гарантия >= 1.0
    return Math.max(1.0, realScaleDown);
}




class PeerConnection {
    constructor(target, ws) {
        let config = { 
            iceServers: [
                { urls: ["stun:stun.l.google.com:19302"] },
            ] 
        }
        this.ws = ws;
        this.target = target;
        this.pc = new RTCPeerConnection(config);
        this.is_restarting = false;
        this.pc.onicecandidate = async (e) => {
            if (!e.candidate || e.candidate.candidate === '') { return };
            this.ws.send(JSON.stringify({
                kind: "rtc",
                target,
                type: "candidate",
                candidate: event.candidate.candidate,
            }));
        }
        this.pc.oniceconnectionstatechange = async (e) => {
            switch (this.pc.iceConnectionState) {
                case 'disconnected': {
                    setTimeout(async () => {
                        if (this.pc.iceConnectionState !== "connected") {
                            await this.restart_ice()
                        }
                    }, 3000);
                    break;
                }
                case 'failed': {
                    await this.restart_ice();
                    break;
                }
            }
        }
    }
    async restart_ice() {
        if (this.is_restarting) { return; }
        this.is_restarting = true;
        this.pc.restartIce();
        let offer = await this.pc.createOffer({iceRestart: true});
        await this.pc.setLocalDescription(offer);
        this.ws.send(JSON.stringify({
            kind: 'rtc',
            target: this.target,
            type: 'ice_restart',
            sdp: offer.sdp
        }));
        this.is_restarting = false;
    }
    async add_ice_candidate(message) {
        const iceCandidate = new RTCIceCandidate({
            candidate: message.candidate,
            sdpMid: message.sdp_mid,
            sdpMLineIndex: message.sdp_mline_index
        });
        await this.pc.addIceCandidate(iceCandidate);
    }
    async receive_answer(message) {
        if (this.pc.signalingState === "have-local-offer") {
            await this.pc.setRemoteDescription(new RTCSessionDescription(message));
        }
    }
}

class SubscriberConnection extends PeerConnection {
    constructor(ws, users) {
        super('subscriber', ws);
        this.pc.ontrack = ({ streams, track }) => {
            const remote_stream = streams[0];
            const user_id = remote_stream.id.split("_")[1];
            let user = users.value.get(user_id);
            if (!!user) {
                user.stream.addTrack(track);
                return;
            }
            users.value.set(user_id, {
                stream: remote_stream,
                isLocal: false
            });
        };
        this.signaling_queue = Promise.resolve();
    }
    async create_answer(sdp) {
        this.signaling_queue = this.signaling_queue.then(() => this.create_answer_task(sdp));
        return this.signaling_queue;
    }
    async create_answer_task(sdp) {
        try {
            await this.pc.setRemoteDescription(new RTCSessionDescription({
                type: 'offer',
                sdp: sdp
            }));
            const answer = await this.pc.createAnswer();
            await this.pc.setLocalDescription(answer);
            this.ws.send(JSON.stringify({
                kind: "rtc",
                target: this.target,
                type: 'answer',
                sdp: answer.sdp
            }));
        } catch (error) {
            console.error("Ошибка при обработке SFU Offer:", error);
        }
    }
}

class PublisherConnection extends PeerConnection {
    constructor(ws) {
        super('publisher', ws);
        this.pc.onconnectionstatechange = (e) => {
            switch (this.pc.iceConnectionState) {
                case 'failed':
                case 'closed':
                case 'disconnected': {
                    this.disconnected = true;
                    if (!!this.video_transceiver) { return; }
                    let parameters = this.video_transceiver.sender.getParameters();
                    parameters.encodings.forEach(e => {
                        e.active = false;
                    });
                    this.video_transceiver.sender.setParameters(parameters);
                    break;
                }
                case 'connected': {
                    if (!this.disconnected) { return; }
                    this.disconnected = false;
                    if (!!this.video_transceiver) { return; }
                    let parameters = this.video_transceiver.sender.getParameters();
                    parameters.encodings.forEach(e => {
                        e.active = true;
                    });
                    this.video_transceiver.sender.setParameters(parameters);
                    break;
                }
            }
        }
    }
    async add_tracks(video_stream, audio_stream) {
        let is_any_stream = video_stream || audio_stream;
        if (video_stream) {
            const deviceType = getDeviceType();
            const bitrateSettings = {
                low: deviceType === 'desktop' ?  400_000 : 100_000,      
                mid: deviceType === 'desktop' ?  2_000_000 : 300_000,   
                high: deviceType === 'desktop' ?  4_000_000 : 500_000   
            };
            const encodings = [
                {
                    rid: 'low',
                    maxBitrate: bitrateSettings.low,
                    scaleResolutionDownBy: getRoundedResolution(BASE_HEIGHT, BASE_WIDTH, 8.0),
                    maxFramerate: 24,
                },
                {
                    rid: 'mid',
                    maxBitrate: bitrateSettings.mid,
                    scaleResolutionDownBy: getRoundedResolution(BASE_HEIGHT, BASE_WIDTH, 4.0),
                    maxFramerate: 30
                },
                {
                    rid: 'high',
                    maxBitrate: bitrateSettings.high,
                    scaleResolutionDownBy: getRoundedResolution(BASE_HEIGHT, BASE_WIDTH, 1),
                    maxFramerate: 30
                }
            ];
            const video_track = video_stream.getVideoTracks()[0];
            this.video_transceiver = this.pc.addTransceiver(video_track, {
                direction: 'sendonly',
                sendEncodings: encodings
            });
        }
        if (audio_stream) {
            const audio_track = audio_stream.getAudioTracks()[0];
            this.pc.addTransceiver(audio_track, { direction: 'sendonly' });
        }
        if (is_any_stream) {
            let offer = await this.pc.createOffer();
            await this.pc.setLocalDescription(offer);
            this.ws.send(JSON.stringify({
                kind: "rtc",
                target: this.target,
                type: "offer",
                sdp: offer.sdp
            }));
        }
    }
}