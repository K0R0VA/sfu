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
        const videoTrack = stream.getVideoTracks()[0];
        this.publisher_pc.addTransceiver(videoTrack, {
            direction: 'sendonly',
            sendEncodings: [
                    {
                    rid: 'low',
                    maxBitrate: 150000,          // Подняли с 70k для четких 180p
                    scaleResolutionDownBy: 8.0,  
                    maxFramerate: 60,
                },
                {
                    rid: 'mid',
                    maxBitrate: 500000,          // Подняли с 250k для стабильных 360p
                    scaleResolutionDownBy: 4.0,  //
                    maxFramerate: 60,
                },
                {
                    rid: 'high',
                    maxBitrate: 1800000,         // Подняли с 1M. Для хорошего 720p30 нужно ~1.5-2.0 Mbps
                    scaleResolutionDownBy: 1.0,  // 1280x720
                    maxFramerate: 120,
                }
            ]
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