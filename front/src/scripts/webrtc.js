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
        this.addTracks = false;
        pc.ontrack = ({ streams, track }) => {
            const remoteStream = streams[0];
            const rawId = remoteStream.id;
            console.log(this.addTracks);
            if (this.addTracks) {
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
    allowAddTracks() {
        this.addTracks = true;
    }
}