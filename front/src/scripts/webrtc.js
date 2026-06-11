export class PeerConnection {
    constructor(users, roomStatus, ws) {
        let config = { 
            iceServers: [
                { urls: ["stun:stun.l.google.com:19302"] },
            ] 
        }
        const subscriber_pc = new RTCPeerConnection(config);
        const publisher_pc = new RTCPeerConnection(config);
        subscriber_pc.ontrack = ({ streams, track }) => {
            console.log('new track', streams);
            const remoteStream = streams[0];
            const rawId = remoteStream.id;
            let user = users.value.get(rawId);
            if (!!user) {
                user.stream.addTrack(track);
                return;
            }
            users.value.set(rawId, {
                stream: remoteStream,
                isLocal: false
            });
            roomStatus.value = `Участников: ${users.value.length}`;
        };
        publisher_pc.onicecandidate = (event) => {
            if (event.candidate && event.candidate.candidate) {
                this.ws.send(JSON.stringify({
                    target: "publisher",
                    type: "candidate",
                    candidate: event.candidate.candidate,
                }));
            }
        };

        subscriber_pc.onicecandidate = (event) => {
            if (event.candidate && event.candidate.candidate) {
                this.ws.send(JSON.stringify({
                    target: "subscriber",
                    type: "candidate",
                    candidate: event.candidate.candidate,
                }));
            }
        };
        this.signaling_queue = Promise.resolve();
        this.subscriber_pc = subscriber_pc;
        this.publisher_pc = publisher_pc;
        this.ws = ws;
    }
    async add_ice_candidate(message) {
        const iceCandidate = new RTCIceCandidate({
            candidate: message.candidate,
            sdpMid: message.sdp_mid,
            sdpMLineIndex: message.sdp_mline_index
        });
        await this[`${message.target}_pc`].addIceCandidate(iceCandidate);
    }
    async create_answer(sdp) {
        this.signaling_queue = this.signaling_queue.then(() => this.create_answer_task(sdp));
        return this.signaling_queue;
    }
    async create_answer_task(sdp) {
        try {
            console.log('new offer');
            await this.subscriber_pc.setRemoteDescription(new RTCSessionDescription({
                type: 'offer',
                sdp: sdp
            }));
            const answer = await this.subscriber_pc.createAnswer();
            await this.subscriber_pc.setLocalDescription(answer);
            this.ws.send(JSON.stringify({
                target: 'subscriber',
                type: 'answer',
                sdp: answer.sdp
            }));
        } catch (error) {
            console.error("Ошибка при обработке SFU Offer:", error);
        }
    }
    async add_stream(stream) {
        const deviceType = getDeviceType();
        const isMobile = deviceType === 'mobile';
        
        // Настройки битрейта в зависимости от устройства
        const bitrateSettings = isMobile ? {
            low: 75000,      // 80k вместо 150k
            mid: 200_000,     // 250k вместо 500k  
            high: 600_000     // 800k вместо 1800k (в 2.25 раза ниже)
        } : {
            low: 150000,
            mid: 350_000,
            high: 1_200_000
        };
        
        const encodings = [
            {
                rid: 'low',
                maxBitrate: bitrateSettings.low,
                scaleResolutionDownBy: getRoundedResolution(BASE_HEIGHT, BASE_WIDTH, 4.0),
                maxFramerate: 30,  // На телефонах меньше FPS
            },
            {
                rid: 'mid',
                maxBitrate: bitrateSettings.mid,
                scaleResolutionDownBy: getRoundedResolution(BASE_HEIGHT, BASE_WIDTH, 2.0),
                maxFramerate: 60,
            },
            {
                rid: 'high',
                maxBitrate: bitrateSettings.high,
                scaleResolutionDownBy: 1.0,
                maxFramerate: 60,
            }
        ];
        const videoTrack = stream.getVideoTracks()[0];
        this.publisher_pc.addTransceiver(videoTrack, {
            direction: 'sendonly',
            sendEncodings: encodings
        });
        const audioTrack = stream.getAudioTracks()[0];
        this.publisher_pc.addTransceiver(audioTrack, { direction: 'sendonly' });
        let offer = await this.publisher_pc.createOffer();
        await this.publisher_pc.setLocalDescription(offer);
        this.ws.send(JSON.stringify({
            target: "publisher",
            type: "offer",
            sdp: offer.sdp
        }));
    }
    async receive_answer(message) {
        if (this.publisher_pc.signalingState === "have-local-offer") {
            await this.publisher_pc.setRemoteDescription(new RTCSessionDescription(message));
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

export function getDeviceType() {
    const ua = navigator.userAgent;
    if (/(tablet|ipad|playbook|silk)|(android(?!.*mobi))/i.test(ua)) {
        return 'tablet';
    }
    if (/Mobile|Android|iP(hone|od)|IEMobile|BlackBerry|Kindle|Silk-Accelerated|(hpw|web)OS|Opera M(obi|ini)/.test(ua)) {
        return 'mobile';
    }
    return 'desktop';
}
