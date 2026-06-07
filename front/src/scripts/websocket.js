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
                const stream = await navigator.mediaDevices.getUserMedia({ 
                    video: {
                        width: { ideal: 1280 },
                        height: { ideal: 720 },
                    },
                    audio: {
                        echoCancellation: true,        // Подавление эха (критически важно!)
                        noiseSuppression: true,        // Подавление шума
                        autoGainControl: true,         // Автоматическая регулировка громкости
                        sampleRate: { ideal: 48000 },  // Частота дискретизации
                        sampleSize: { ideal: 16 },     // Битность
                        channelCount: { ideal: 1 }     // Моно (достаточно для голоса)
                    }
                });
                const videoTrack = stream.getVideoTracks()[0];
                this.peer_connection.pc.addTransceiver(videoTrack, {
                    direction: 'sendonly',
                    sendEncodings: [
                          {
                            rid: 'low',
                            maxBitrate: 150000,          // Подняли с 70k для четких 180p
                            scaleResolutionDownBy: 4.0,  // 320x180
                            maxFramerate: 15             // Ограничение FPS экономит трафик слабых клиентов
                            
                        },
                        {
                            rid: 'mid',
                            maxBitrate: 500000,          // Подняли с 250k для стабильных 360p
                            scaleResolutionDownBy: 2.0,  // 640x360
                            maxFramerate: 30
                        },
                        {
                            rid: 'high',
                            maxBitrate: 1800000,         // Подняли с 1M. Для хорошего 720p30 нужно ~1.5-2.0 Mbps
                            scaleResolutionDownBy: 1.0,  // 1280x720
                            maxFramerate: 30
                        }
                    ]
                });
                const audioTrack = stream.getAudioTracks()[0];
                this.peer_connection.pc.addTransceiver(audioTrack, { direction: 'sendonly' });
                await this.peer_connection.create_offer();
                this.users.value.set(this.peer_id, {
                    stream,
                    isLocal: true
                });
                this.isJoined.value = true;
                this.isConnecting.value = false;
                this.roomStatus.value = "Подключено";
                break;
            }
            case 'peer_join': {
                this.peer_connection.addPeer()
                this.roomStatus.value = "Подключаем участника...";
                break;
            }
            case 'peer_left': {
                let peer_id = message.peer_id;
                this.users.value.delete(peer_id);
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
        this.users.value.clear();
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