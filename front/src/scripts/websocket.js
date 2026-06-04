import { PeerConnection } from "./webrtc";

export class UserWebsocket {
    constructor(users, roomStatus, isConnecting, isJoined) {
            let ws = createWebSocket();
            let peer_connection = new PeerConnection(users, roomStatus, ws);
            ws.onopen = () => {
                console.log(`🟢 WebSocket соединен`);
                roomStatus.value = "Соединение установлено";
            };
            ws.onmessage = async (event) => {
                const message = JSON.parse(event.data);
                switch (message.type) {
                    case 'welcome': {
                        this.peer_id = message.assigned_peer_id;
                        roomStatus.value = "Получение медиапотока...";
                        const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
                        const videoTrack = stream.getVideoTracks()[0];
                        peer_connection.set_stream_id(this.peer_id);
                        stream.getTracks().forEach(track => {
                            peer_connection.pc.addTrack(track, stream);
                        });
                        users.value.push({
                            id: this.peer_id,
                            stream,
                            isLocal: true
                        });
                        isJoined.value = true;
                        isConnecting.value = false;
                        roomStatus.value = "Подключено";
                    }
                    case 'peer_joined': {
                        peer_connection.pc.addTransceiver('video', { direction: 'recvonly' });
                        roomStatus.value = "Подключаем участника...";
                    }
                    case 'peer_left': {
                        let peer_id = message.id;
                        users.value = users.value.filter(user => user.id != peer_id);
                    }
                    case 'answer': {
                        if (peer_connection.pc.signalingState === "have-local-offer") {
                            await peer_connection.pc.setRemoteDescription(new RTCSessionDescription(message));
                        }
                    }
                    default: {}
            }
        };
        ws.onclose = () => {
            console.log("🔴 Соединение закрыто");
            isConnecting.value = false;
            roomStatus.value = "Соединение потеряно";
        };
    }
}

function createWebSocket () {
    const serverIP = window.location.hostname;
    if (serverIP === 'localhost') {
        return new WebSocket(`ws://${serverIP}:8080/ws`);
    } else {
        return new WebSocket(`wss://${serverIP}/ws`);
    }
};