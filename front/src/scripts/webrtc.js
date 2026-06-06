export class PeerConnection {
    constructor(users, roomStatus, ws) {
        const pc = new RTCPeerConnection({ 
            iceServers: [
                { urls: ["stun:stun.l.google.com:19302"] },
            ] 
        });
        pc.ontrack = ({ streams, track }) => {
            if (streams.length == 0) return;
            const remoteStream = streams[0];
            const rawId = remoteStream.id;
            let user = users.value.get(rawId);
            if (!!user) {
                remoteStream.getTracks(track => user.stream.addTrack(track));
                return;
            }
            users.value.set(rawId, {
                stream: remoteStream,
                isLocal: false
            });
            roomStatus.value = `Участников: ${users.value.length}`;
        };
        this.makingOffer = false;
        pc.onnegotiationneeded = async (event) => {
            if (this.makingOffer) return;
            try {
                this.makingOffer = true;
                const offer = await this.pc.createOffer();
                await this.pc.setLocalDescription(offer);
                console.log(offer);
                ws.send(JSON.stringify(offer));
            } catch (err) {
                console.log(err);
                this.makingOffer = false;
            }
            finally {
                this.makingOffer = false;
            }
        };
        this.pc = pc
    }
}