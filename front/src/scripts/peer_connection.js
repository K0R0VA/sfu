export class PeerConnection {
    constructor(target, ws) {
        let config = { 
            iceServers: [
                { urls: ["stun:stun.sipnet.ru:3478"] },
            ] 
        }
        this.ws = ws;
        this.target = target;
        this.pc = new RTCPeerConnection(config);
        this.is_restarting = false;
        this.pc.onicecandidate = async (e) => {
            if (!e.candidate) { return };
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
        console.log('restart ice');
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