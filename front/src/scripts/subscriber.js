import { PeerConnection } from "./peer_connection";

export class SubscriberConnection extends PeerConnection {
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