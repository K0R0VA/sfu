export class PeerConnection {
    constructor(users, roomStatus, ws) {
        const pc = new RTCPeerConnection({ 
            iceServers: [
                { urls: ["stun:stun.l.google.com:19302"] },
                {
                    urls: ["turn:192.168.0.106:3478"],
                    username: "admin",
                    credential: "secretpassword"
                }
            ] 
        });
        this.stream_id = '';
        pc.ontrack = ({ streams }) => {
            const remoteStream = streams[0];
            const rawId = remoteStream.id;
            console.log('rawId: ', rawId, '; stream_id: ', this.stream_id)
            if (rawId !== this.stream_id) {
                console.log
                users.value.push({
                    id: rawId,
                    stream: remoteStream,
                    isLocal: false
                });
                roomStatus.value = `Участников: ${users.value.length}`;
            }
        };
        pc.onnegotiationneeded = async () => {
            await pc.setLocalDescription();
            ws.send(JSON.stringify(pc.localDescription));
        };
        this.pc = pc
    }
    set_stream_id(stream_id) {
        this.stream_id = stream_id;
    }
}