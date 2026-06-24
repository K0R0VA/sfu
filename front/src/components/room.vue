<template>
  <div class="room-view">
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
