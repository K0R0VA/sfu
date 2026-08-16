import { PeerConnection } from "./peer_connection";
import { getDeviceType } from "./websocket";

export class PublisherConnection extends PeerConnection {
    constructor(ws) {
        super('publisher', ws);
        this.pc.onconnectionstatechange = (e) => {
            switch (this.pc.iceConnectionState) {
                case 'failed':
                case 'closed':
                case 'disconnected': {
                    this.disconnected = true;
                    if (!!this.video_transceiver) { return; }
                    let parameters = this.video_transceiver.sender.getParameters();
                    parameters.encodings.forEach(e => {
                        e.active = false;
                    });
                    this.video_transceiver.sender.setParameters(parameters);
                    break;
                }
                case 'connected': {
                    if (!this.disconnected) { return; }
                    this.disconnected = false;
                    if (!!this.video_transceiver) { return; }
                    let parameters = this.video_transceiver.sender.getParameters();
                    parameters.encodings.forEach(e => {
                        e.active = true;
                    });
                    this.video_transceiver.sender.setParameters(parameters);
                    break;
                }
            }
        }
    }
    async add_tracks(video_stream, audio_stream) {
        let is_any_stream = video_stream || audio_stream;
        if (video_stream) {
            const deviceType = getDeviceType();
            const bitrateSettings = {
                low: deviceType === 'desktop' ?  400_000 : 100_000,      
                mid: deviceType === 'desktop' ?  2_000_000 : 300_000,   
                high: deviceType === 'desktop' ?  4_000_000 : 500_000   
            };
            const encodings = [
                {
                    rid: 'low',
                    maxBitrate: bitrateSettings.low,
                    scaleResolutionDownBy: getRoundedResolution(4.0),
                    maxFramerate: 24,
                },
                {
                    rid: 'mid',
                    maxBitrate: bitrateSettings.mid,
                    scaleResolutionDownBy: getRoundedResolution(2.0),
                    maxFramerate: 30,
                },
                {
                    rid: 'high',
                    maxBitrate: bitrateSettings.high,
                    scaleResolutionDownBy: getRoundedResolution(1),
                    maxFramerate: 30,
                }
            ];
            const video_track = video_stream.getVideoTracks()[0];
            this.video_transceiver = this.pc.addTransceiver(video_track, {
                direction: 'sendonly',
                sendEncodings: encodings
            });
            const codecs = RTCRtpSender.getCapabilities('video').codecs;
            // Фильтруем, оставляя только VP8 или H.264 на первом месте
            const preferredCodecs = codecs.filter(c => c.mimeType.toLowerCase() === 'video/vp8');
            this.video_transceiver.setCodecPreferences(preferredCodecs);
        }
        if (audio_stream) {
            const audio_track = audio_stream.getAudioTracks()[0];
            this.pc.addTransceiver(audio_track, { direction: 'sendonly' });
        }
        if (is_any_stream) {
            let offer = await this.pc.createOffer();
            await this.pc.setLocalDescription(offer);
            this.ws.send(JSON.stringify({
                kind: "rtc",
                target: this.target,
                type: "offer",
                sdp: offer.sdp
            })); 
        }
    }
}


function getRoundedResolution(targetScale, alignment = 32) {
    const baseWidth = 1080;
    const baseHeight = 1920;
    if (targetScale <= 1.0) {
        // Для high слоя - никаких округлений
        return 1.0;
    }
    
    let targetWidth = baseWidth / targetScale;
    let targetHeight = baseHeight / targetScale;
    
    let roundedWidth = Math.round(targetWidth / alignment) * alignment;
    let roundedHeight = Math.round(targetHeight / alignment) * alignment;
    
    // Убеждаемся, что не увеличиваем разрешение
    if (roundedWidth > baseWidth || roundedHeight > baseHeight) {
        return targetScale; // Возвращаем исходный коэффициент
    }
    
    let realScaleDown = baseWidth / roundedWidth;
    
    // Гарантия >= 1.0
    return Math.max(1.0, realScaleDown);
}