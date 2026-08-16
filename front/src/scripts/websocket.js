import { SmartWebSocket } from "./smart_websocket";
import { WebrtcConnection } from "./webrtc";

export class UserWebsocket {
    constructor(users, room_id, room_status, room_name) {
        console.log(room_id);
        this.ws = createWebSocket(room_id.value);
        this.webrtc_connection = new WebrtcConnection(users, this.ws);
        this.room_status = room_status;
        this.room_name = room_name;
        this.users = users;
    }
    init() {
        this.ws.on_message(async (event) => {
            const message = JSON.parse(event.data);
            switch (message.kind) {
                case 'room_info': {
                    this.room_name.value = message.name;
                    break;
                }
                case 'welcome': {
                    this.peer_id = message.peer_id;
                    this.room_status.value = "Получение медиапотока...";
                    const video_stream = await this.getMedia({ 
                        video: {
                            width: { ideal: 1280 },
                            height: { ideal: 720 },
                            frameRate: { ideal: 30 }
                        }
                    });
                    const audio_stream = await this.getMedia({
                        audio: {
                            echoCancellation: true,        // Подавление эха (критически важно!)
                            noiseSuppression: true,        // Подавление шума
                            autoGainControl: true,         // Автоматическая регулировка громкости
                            sampleRate: { ideal: 48000 },  // Частота дискретизации
                            sampleSize: { ideal: 16 },     // Битность
                            channelCount: { ideal: 1 }     // Моно (достаточно для голоса)
                        }
                    });
                    await this.webrtc_connection.publisher.add_tracks(video_stream, audio_stream);
                    if (!!video_stream) {
                        this.users.value.set(this.peer_id, {
                            stream: video_stream,
                            isLocal: true
                        });
                    }
                    this.room_status.value = "Подключено";
                    break;
                }
                case 'rtc': {
                    switch (message.type) {
                        case 'candidate': {
                            await this.webrtc_connection.add_ice_candidate(message);
                            break;
                        }
                        case 'offer': {
                            await this.webrtc_connection.subscriber.create_answer(message.sdp);
                            break;
                        }
                        case 'answer': {
                            await this.webrtc_connection.receive_answer(message);
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
        });
    }
    async getMedia(request) {
        let video_stream;
        try {
            video_stream = await navigator.mediaDevices.getUserMedia(request);
        }
        catch {
            this.room_status.value = 'Ошибка получения медиа потока';
        }
        finally {
            return video_stream
        }
    }
    disconnect() {
        try {
            this.ws.close();
            this.webrtc_connection.publisher.pc.close();
            this.webrtc_connection.subscriber.pc.close();
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
        return new SmartWebSocket(`ws://${hostname}:8080/api/room/${room_id}`);
    } else {
        return new SmartWebSocket(`wss://${hostname}/api/room/${room_id}`);
    }
};

export function getDeviceType() {
    const ua = navigator.userAgent;
    // if (/(tablet|ipad|playbook|silk)|(android(?!.*mobi))/i.test(ua)) {
    //     return 'tablet';
    // }
    if (/Mobile|Android|iP(hone|od)|IEMobile|BlackBerry|Kindle|Silk-Accelerated|(hpw|web)OS|Opera M(obi|ini)/.test(ua)) {
        return 'mobile';
    }
    return 'desktop';
}