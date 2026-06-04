import { PeerConnection } from "./webrtc";

export class UserWebsocket {
    constructor(users, roomStatus, isConnecting, isJoined) {
        this.ws = createWebSocket();
        this.peer_connection = new PeerConnection(users, roomStatus, this.ws);
        this.roomStatus = roomStatus;
        this.isConnecting = isConnecting;
        this.isJoined = isJoined;
        this.users = users;
    }
    init() {
        this.ws.onopen = () => {
            console.log(`🟢 WebSocket соединен`);
            this.roomStatus.value = "Соединение установлено";
        };
        this.ws.onmessage = async (event) => {
            const message = JSON.parse(event.data);
            switch (message.type) {
            case 'welcome': {
                this.peer_id = message.assigned_peer_id;
                this.roomStatus.value = "Получение медиапотока...";
                const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
                stream.getTracks().forEach(track => {
                    this.pc.addTrack(track, stream);
                });
                this.users.value.push({
                    id: this.peer_id,
                    stream,
                    isLocal: true
                });
                this.isJoined.value = true;
                this.isConnecting.value = false;
                this.roomStatus.value = "Подключено";
                break;
            }
            case 'peer_join': {
                this.peer_connection.pc.addTransceiver('video', { direction: 'recvonly' });
                // this.peer_connection.pc.addTransceiver('audio', { direction: 'recvonly' });
                this.roomStatus.value = "Подключаем участника...";
                break;
            }
            case 'peer_left': {
                let peer_id = message.peer_id;
                this.users.value = this.users.value.filter(user => user.id != peer_id);
                break;
            }
            case 'answer': {
                if (this.peer_connection.pc.signalingState === "have-local-offer") {
                    await this.peer_connection.pc.setRemoteDescription(new RTCSessionDescription(message));
                }
                break;
            }
        }};
        this.ws.onclose = () => {
            console.log("🔴 Соединение закрыто");
            this.isConnecting.value = false;
            this.roomStatus.value = "Соединение потеряно";
        };
    }
    disconnect() {
        this.ws.close();
        this.peer_connection.pc.close();
        this.users.value = [];
        this.isJoined.value = false;
        this.isJoined.value = false;
        this.roomStatus.value = 'Готов к подключению';
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