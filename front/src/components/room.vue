<template>
  <div class="room-view">
    <!-- Панель управления профилем сети -->
    <div class="network-controls">
      <label for="network-profile">Эмуляция сети:</label>
      <select 
        id="network-profile" 
        v-model="selectedProfile" 
        @change="applyNetworkProfile"
      >
        <option value="original">Без ограничений (High)</option>
        <option value="mid">Mid</option>
        <option value="low">Low</option>
      </select>
    </div>

    <VideoGrid 
      :users="users"
      @set-video-src="setVideoSrc"
    />
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch, inject } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import VideoGrid from '../components/video_grid.vue';
import { connect_websocket, setVideoSrc } from '../scripts/index.js';

const users_map = ref(new Map());
const route = useRoute();
const router = useRouter();
const room_id = computed(() => route.params.room_id);


// Получаем состояние от App
const { room_name, room_status, is_in_room } = inject('roomState');
let websocket = null;

const users = computed(() => {
  return Array.from(users_map.value.entries()).map(([id, data]) => ({
    id: id,
    stream: data.stream,
    isLocal: data.isLocal,
  }));
});

const selectedProfile = ref('original');

// Подключение к комнате
const connectToRoom = () => {
  if (websocket) {
    websocket.disconnect();
    users_map.value.clear();
  }
  websocket = connect_websocket(
    users_map,
    room_id,
    room_status,
    room_name
  );
  is_in_room.value = true;
};

// Обработка закрытия страницы
const handleBeforeUnload = (event) => {
  if (is_in_room.value) {
    event.preventDefault();
    event.returnValue = 'Вы уверены, что хотите покинуть комнату?';
  }
};

// Функция изменения параметров WebRTC сендера
const applyNetworkProfile = async () => {
  try {
    const video_sender = websocket.peer_connection.video_transceiver.sender;
    if (!video_sender) return;

    const parameters = video_sender.getParameters();
    
    if (!parameters.encodings || parameters.encodings.length === 0) {
      parameters.encodings = [{}];
    }
    const low_layer = parameters.encodings[0];
    const mid_layer = parameters.encodings[1];
    const high_layer = parameters.encodings[2];

    switch (selectedProfile.value) {
      case 'low':
        low_layer.maxBitrate = 150_000; 
        mid_layer.active = false; 
        high_layer.active = false; 
        break;
      case 'mid':
        low_layer.maxBitrate = 400_000;
        mid_layer.active = true; 
        high_layer.active = false; 
        break;
      case 'original':
      default:
        // Полностью очищаем лимиты для возврата к дефолту браузера
        low_layer.maxBitrate = 400_000;
        mid_layer.active = true; 
        high_layer.active = true; 
        break;
    }
    await video_sender.setParameters(parameters);
  } catch (error) {
    console.error('[WebRTC] Ошибка изменения параметров трека:', error);
  }
};

onMounted(() => {
    connectToRoom() 
});

onUnmounted(() => {
  if (websocket) {
    websocket.disconnect();
  }
  users_map.value.clear();
  room_status.value = 'Покинул комнату';
  is_in_room.value = false;
  router.push('/');
});


watch(room_id, () => {
  connectToRoom();
});
</script>

<style src="../styles/room.css" scoped></style>
