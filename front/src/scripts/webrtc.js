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
        pc.ontrack = ({ streams, track }) => {
            const remoteStream = streams[0];
            const rawId = remoteStream.id;
            users.value.push({
                id: rawId,
                stream: remoteStream,
                isLocal: false
            });
            roomStatus.value = `Участников: ${users.value.length}`;
        };
        pc.onnegotiationneeded = async () => {
            await pc.setLocalDescription();
            ws.send(JSON.stringify(pc.localDescription));
        };
        this.pc = pc
    }
}