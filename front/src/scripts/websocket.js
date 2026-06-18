import { PeerConnection, getDeviceType } from "./webrtc";

export class UserWebsocket {
    constructor(users, roomStatus, isConnecting, isJoined, error) {
        this.ws = createWebSocket();
        this.peer_connection = new PeerConnection(users, roomStatus, this.ws);
        this.roomStatus = roomStatus;
        this.isConnecting = isConnecting;
        this.isJoined = isJoined;
        this.users = users;
        this.error = error;
    }
    init() {
        this.ws.onopen = () => {
            console.log(`🟢 WebSocket соединен`);
            this.roomStatus.value = "Соединение установлено";
        };
        this.ws.onmessage = async (event) => {
            const message = JSON.parse(event.data);
            switch (message.kind) {
                case 'welcome': {
                    this.peer_id = message.peer_id;
                    this.roomStatus.value = "Получение медиапотока...";
                    const video_stream = await navigator.mediaDevices.getUserMedia({ 
                        video: {
                            width: { ideal: 1280 },
                            height: { ideal: 720 },
                            frameRate: { ideal: 60 }
                        }
                    });
                    const audio_stream = await navigator.mediaDevices.getUserMedia({
                        audio: {
                            echoCancellation: true,        // Подавление эха (критически важно!)
                            noiseSuppression: true,        // Подавление шума
                            autoGainControl: true,         // Автоматическая регулировка громкости
                            sampleRate: { ideal: 48000 },  // Частота дискретизации
                            sampleSize: { ideal: 16 },     // Битность
                            channelCount: { ideal: 1 }     // Моно (достаточно для голоса)
                        }
                    });
                    await this.peer_connection.add_tracks(video_stream, audio_stream);
                    const device_type = getDeviceType();
                    this.ws.send(JSON.stringify({
                        kind: "connect",
                        device_type
                    }));
                    this.users.value.set(this.peer_id, {
                        stream: video_stream,
                        isLocal: true
                    });
                    this.isJoined.value = true;
                    this.isConnecting.value = false;
                    this.roomStatus.value = "Подключено";
                    break;
                }
                case 'rtc': {
                    switch (message.type) {
                        case 'candidate': {
                            await this.peer_connection.add_ice_candidate(message);
                            break;
                        }
                        case 'offer': {
                            await this.peer_connection.create_answer(message.sdp);
                            break;
                        }
                        case 'ice_restart': {
                            await this.peer_connection.restart_ice(message.target);
                            break;
                        }
                        case 'answer': {
                            await this.peer_connection.receive_answer({type: message.type, sdp: message.sdp});
                            break;
                        }
                    }
                    break;
                }
                case 'peer_left': {
                    let peer_id = message.peer_id;
                    this.users.value.delete(peer_id);
                    break;
                }
            }
        };
        this.ws.onclose = () => {
            console.log("🔴 Соединение закрыто");
            this.isConnecting.value = false;
            this.roomStatus.value = "Соединение потеряно";
        };
    }
    disconnect() {
        try {
            this.ws.close();
            this.peer_connection.publisher_pc.close();
            this.peer_connection.subscriber_pc.close();
        }
        catch (e) {
            console.log(e);
        }
        finally {
            this.peer_id = null;
            this.users.value.clear();
            this.isJoined.value = false;
            this.isJoined.value = false;
            this.roomStatus.value = 'Готов к подключению';
        }
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