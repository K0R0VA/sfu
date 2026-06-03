<script setup>
import { ref } from 'vue';

const roomStatus = ref("Подключение к медиа-серверу на акторах");
const isJoined = ref(false);
const isConnecting = ref(false);
const users = ref([]); // Массив участников: { id: String, stream: MediaStream, isLocal: Boolean }

let ws = null;
let pc = null;
let myPeerId = null;
let localMediaStream = null;

// Монтируем MediaStream в HTML5 тег video внутри v-for
const setVideoSrc = (el, stream) => {
  if (el && stream && el.srcObject !== stream) {
    el.srcObject = stream;
    el.play().catch(e => console.error("Ошибка автоплея Vite video:", e));
  }
};

const joinRoom = async () => {
  isConnecting.value = true;
  roomStatus.value = "Запрос доступа к камере...";

        let ws = create_ws();
        const pc = new RTCPeerConnection({ iceServers: [
            {
                urls: ["stun:stun.l.google.com:19302"]
            },
            {
                urls: ["turn:192.168.0.106:3478"],
                username: "admin",               
                credential: "secretpassword"
            }
        ] });
        let peer_id;
        let stream_id;
        ws.onopen = async () => {
            console.log(`🟢 WebSocket соединен`);
        };
        pc.ontrack = ({track, streams}) => {
            console.log("🎯 [OnTrack] Прилетел медиа-трек от сервера:", event);
            
            // Ищем среди прилетевших стримов тот, который принадлежит соседу
            // (то есть его ID не равен ID нашей собственной локальной камеры)
            const remoteStream = streams[0];

            const rawId = remoteStream.id; 
            if (rawId != stream_id) {
                console.log("Поток соседа найден:", remoteStream);
                const remotePeerId = rawId.startsWith("stream_") ? rawId.replace("stream_", "") : rawId;
                console.log(`🎯 Рендерим окно для пользователя: ${remotePeerId}`);
                users.value.push({
                    id: stream_id,
                    stream: remoteStream,
                    isLocal: false
                })
            }
        };
        let makingOffer = false;

// 🔥 1. ВСТРОЕННЫЙ ДАТЧИК ИЗ MDN (Срабатывает сам, когда мы добавляем трансиверы!)
        pc.onnegotiationneeded = async () => {
            try {
                makingOffer = true;
                console.log("🔄 [MDN Паттерн] Датчик onnegotiationneeded сработал! Создаем Offer автоматически...");
                
                // setLocalDescription() без аргументов сам создаст идеальный Оффер
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
            if (message.type == 'welcome') {
                peer_id = message.assigned_peer_id;
                console.log(`🟢 Мой ID: ${peer_id}`);
                const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
                const videoTrack = stream.getVideoTracks()[0];
                stream_id = videoTrack.id;
                stream.getTracks().forEach(track => {
                    pc.addTrack(track, stream);
                })
                users.value.push({
                    id: peer_id,
                    stream,
                    isLocal: true
                });
                isJoined.value = true
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
            videoGrid.innerHTML = "";
            startBtn.disabled = false;
        };
        function create_ws() {
          const serverIP = window.location.hostname; 
          if (serverIP == 'localhost') {
              return new WebSocket(`ws://${serverIP}:8080/ws`)
          } else {
              return new WebSocket(`wss://${serverIP}/ws`)
          }
      }
    };
    
</script>

<<template>
  <div class="app-wrapper">
    <!-- Шапка интерфейса -->
    <header class="main-header">
      <div class="logo-zone">
        <div class="live-indicator" :class="{ joined: isJoined, connecting: isConnecting }"></div>
        <h1>Rust Media SFU Engine</h1>
      </div>
      <div class="status-badge">{{ roomStatus }}</div>
      <button 
        @click="joinRoom" 
        :disabled="isJoined || isConnecting" 
        class="action-btn"
        :class="{ active: isJoined }"
      >
        <span v-if="isConnecting" class="spinner"></span>
        {{ isConnecting ? 'Присоединение...' : isJoined ? 'Вы в комнате' : 'Войти в комнату' }}
      </button>
    </header>

    <!-- Сетка видео -->
    <main class="content-area">
      <!-- Заглушка, если в комнате пусто -->
      <div v-if="users.length === 0" class="empty-state">
        <div class="empty-icon">🎥</div>
        <h3>Комната пуста</h3>
        <p>Нажмите кнопку выше, чтобы запустить камеру и подключиться к акторам Rust.</p>
      </div>

      <div v-else class="video-grid">
        <div 
          v-for="peer in users" 
          :key="peer.id" 
          class="video-card"
          :class="{ 'local-card': peer.isLocal }"
        >
          <div class="video-wrapper">
            <video 
              :ref="el => setVideoSrc(el, peer.stream)"
              autoplay 
              playsinline 
              webkit-playsinline
              :muted="true"
            ></video>
          </div>
          <!-- Стеклянная плашка с ID -->
          <div class="peer-meta">
            <span class="role-dot" :class="{ local: peer.isLocal }"></span>
            <span class="peer-id">{{ peer.isLocal ? 'Вы' : 'Сосед' }}: {{ peer.id.slice(0, 8) }}...</span>
          </div>
        </div>
      </div>
    </main>
  </div>
</template>

<style scoped>
/* Оболочка всего интерфейса */
.app-wrapper {
  display: flex;
  flex-direction: column;
  width: 100vw;
  max-width: 1400px;
  min-height: 100vh;
  padding: 0 20px;
}

/* Стилизация панели управления (Шапки) */
.main-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 20px 0;
  border-bottom: 1px solid #1f1f23;
  margin-bottom: 24px;
}

.logo-zone {
  display: flex;
  align-items: center;
  gap: 12px;
}

.logo-zone h1 {
  font-size: 20px;
  font-weight: 700;
  letter-spacing: -0.5px;
  background: linear-gradient(90deg, #00ea91, #00bfff);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}

/* Пульсирующий индикатор подключения */
.live-indicator {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  background-color: #ef4444;
  box-shadow: 0 0 8px #ef4444;
  transition: all 0.3s ease;
}
.live-indicator.connecting {
  background-color: #f59e0b;
  box-shadow: 0 0 12px #f59e0b;
  animation: pulse 1.5s infinite;
}
.live-indicator.joined {
  background-color: #00ea91;
  box-shadow: 0 0 12px #00ea91;
  animation: pulse 2s infinite;
}

.status-badge {
  font-size: 13px;
  color: #9ca3af;
  background: #16161a;
  padding: 6px 16px;
  border-radius: 20px;
  border: 1px solid #24242b;
}

/* Киберпанк кнопка */
.action-btn {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 24px;
  font-size: 14px;
  font-weight: 600;
  cursor: pointer;
  border: none;
  border-radius: 8px;
  background: #00ea91;
  color: #0a0a0c;
  transition: transform 0.1s ease, background-color 0.2s ease;
}
.action-btn:hover:not(:disabled) {
  background: #00cc7e;
  transform: translateY(-1px);
}
.action-btn:disabled {
  background: #1f1f23;
  color: #4b5563;
  cursor: not-allowed;
}
.action-btn.active {
  background: #24242b;
  color: #00ea91;
  border: 1px solid #00ea91;
  cursor: default;
}

/* Зона контента и сетка */
.content-area {
  flex-grow: 1;
  display: flex;
  flex-direction: column;
  justify-content: center;
  padding-bottom: 40px;
}

.video-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(380px, 1fr));
  gap: 20px;
  width: 100%;
}

/* Умные брейкпоинты для мобилок */
@media (max-width: 480px) {
  .video-grid {
    grid-template-columns: 1fr;
  }
}

/* Карточка стрима (Плитка) */
.video-card {
  position: relative;
  background: #141417;
  border-radius: 14px;
  overflow: hidden;
  aspect-ratio: 16/9;
  border: 1px solid #24242b;
  box-shadow: 0 12px 32px rgba(0, 0, 0, 0.5);
  transition: border-color 0.3s ease;
}
.video-card:hover {
  border-color: #3b3b4f;
}
.video-card.local-card {
  border-color: rgba(0, 234, 145, 0.4);
}

.video-wrapper {
  width: 100%;
  height: 100%;
}
video {
  width: 100%;
  height: 100%;
  object-fit: cover;
  transform: scaleX(-1); /* Зеркалим видео для естественного восприятия */
}

/* Метаданные (Стеклянная плашка) */
.peer-meta {
  position: absolute;
  bottom: 12px;
  left: 12px;
  display: flex;
  align-items: center;
  gap: 8px;
  background: rgba(10, 10, 12, 0.65);
  padding: 6px 14px;
  border-radius: 6px;
  backdrop-filter: blur(8px);
  -webkit-backdrop-filter: blur(8px);
  border: 1px solid rgba(255, 255, 255, 0.05);
}

.role-dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background-color: #3b82f6;
}
.role-dot.local {
  background-color: #00ea91;
}

.peer-id {
  font-size: 12px;
  font-weight: 600;
  color: #f3f4f6;
}

/* Стили пустой комнаты */
.empty-state {
  text-align: center;
  max-width: 400px;
  margin: 100px auto;
  color: #6b7280;
}
.empty-icon {
  font-size: 48px;
  margin-bottom: 16px;
  opacity: 0.5;
}
.empty-state h3 {
  color: #9ca3af;
  margin-bottom: 8px;
}

/* Анимация пульсации */
@keyframes pulse {
  0% { transform: scale(0.95); opacity: 0.5; }
  50% { transform: scale(1.05); opacity: 1; }
  100% { transform: scale(0.95); opacity: 0.5; }
}

/* Крутилка загрузки */
.spinner {
  width: 16px;
  height: 16px;
  border: 2px solid rgba(0, 0, 0, 0.1);
  border-top-color: currentcolor;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}
@keyframes spin {
  to { transform: rotate(360deg); }
}
</style>