import { PeerConnection, getDeviceType } from "./webrtc";

export class UserWebsocket {
    constructor(users, room_id, room_status, room_name) {
        console.log('connect webscocket');
        this.ws = createWebSocket(room_id.value);
        this.peer_connection = new PeerConnection(users, room_status, this.ws);
        this.room_status = room_status;
        this.room_name = room_name;
        this.users = users;
    }
    init() {
        this.ws.onopen = () => {
            console.log(`🟢 WebSocket соединен`);
            this.room_status.value = "Соединение установлено";
        };
        this.ws.onmessage = async (event) => {
            const message = JSON.parse(event.data);
            switch (message.kind) {
                case 'room_info': {
                    this.room_name.value = message.name;
                    break;
                }
                case 'welcome': {
                    this.peer_id = message.peer_id;
                    this.room_status.value = "Получение медиапотока...";
                    const video_stream = await navigator.mediaDevices.getUserMedia({ 
                        video: {
                            width: { ideal: 1280 },
                            height: { ideal: 720 },
                            frameRate: { ideal: 30 }
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
                    this.room_status.value = "Подключено";
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
            this.room_status.value = "Соединение потеряно";
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
            this.room_status.value = 'Готов к подключению';
        }
    }
}

function createWebSocket (room_id) {
    const hostname = window.location.hostname;
    if (hostname === 'localhost') {
        return new WebSocket(`ws://${hostname}:8080/ws/${room_id}`);
    } else {
        return new WebSocket(`wss://${hostname}/ws/${room_id}`);
    }
};