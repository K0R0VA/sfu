import { ref } from 'vue';

export function useWebRTC(users, isJoined, isConnecting, roomStatus) {
  let ws = null;
  let pc = null;
  let myPeerId = null;
  let localMediaStream = null;

  const setVideoSrc = (el, stream) => {
    if (el && stream && el.srcObject !== stream) {
      el.srcObject = stream;
      el.play().catch(e => console.error("Ошибка автоплея Vite video:", e));
    }
  };

  const createWebSocket = () => {
    const serverIP = window.location.hostname;
    if (serverIP === 'localhost') {
      return new WebSocket(`ws://${serverIP}:8080/ws`);
    } else {
      return new WebSocket(`wss://${serverIP}/ws`);
    }
  };

  const joinRoom = async () => {
    isConnecting.value = true;
    roomStatus.value = "Запрос доступа к камере...";

    ws = createWebSocket();
    pc = new RTCPeerConnection({ 
      iceServers: [
        { urls: ["stun:stun.l.google.com:19302"] },
        {
          urls: ["turn:192.168.0.106:3478"],
          username: "admin",
          credential: "secretpassword"
        }
      ] 
    });
    
    let peer_id;
    let stream_id;

    ws.onopen = async () => {
      console.log(`🟢 WebSocket соединен`);
    };

    pc.ontrack = ({ track, streams }) => {
      console.log("🎯 [OnTrack] Прилетел медиа-трек от сервера:", track);
      
      const remoteStream = streams[0];
      const rawId = remoteStream.id;
      
      if (rawId !== stream_id) {
        console.log("Поток соседа найден:", remoteStream);
        const remotePeerId = rawId.startsWith("stream_") ? rawId.replace("stream_", "") : rawId;
        console.log(`🎯 Рендерим окно для пользователя: ${remotePeerId}`);
        
        users.value.push({
          id: stream_id,
          stream: remoteStream,
          isLocal: false
        });
      }
    };

    let makingOffer = false;

    pc.onnegotiationneeded = async () => {
      try {
        makingOffer = true;
        console.log("🔄 [MDN Паттерн] Датчик onnegotiationneeded сработал! Создаем Offer автоматически...");
        
        await pc.setLocalDescription();
        
        console.log("📤 Отправляем автоматический SDP Offer на сервер...");
        ws.send(JSON.stringify(pc.localDescription));
      } catch (err) {
        console.error("Ошибка в паттерне перепереговоров:", err);
      } finally {
        makingOffer = false;
      }
    };

    ws.onmessage = async (event) => {
      const message = JSON.parse(event.data);
      
      if (message.type === 'welcome') {
        peer_id = message.assigned_peer_id;
        console.log(`🟢 Мой ID: ${peer_id}`);
        
        const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
        const videoTrack = stream.getVideoTracks()[0];
        stream_id = videoTrack.id;
        
        stream.getTracks().forEach(track => {
          pc.addTrack(track, stream);
        });
        
        users.value.push({
          id: peer_id,
          stream,
          isLocal: true
        });
        
        isJoined.value = true;
      } 
      else if (message.type === 'peer_joined') {
        console.log("🎯 [OnTrack] Прилетел новый пользователь", message);
        pc.addTransceiver('video', { direction: 'recvonly' });
        return;
      } 
      else if (message.type === 'answer') {
        if (pc.signalingState === "have-local-offer") {
          await pc.setRemoteDescription(new RTCSessionDescription(message));
          console.log("🟢 Стейт-машина успешно переведена в stable");
        }
      }
    };

    ws.onclose = () => {
      console.log("🔴 Соединение закрыто");
      isConnecting.value = false;
    };
  };

  return {
    joinRoom,
    setVideoSrc
  };
}