<template>
  <div class="room-list-page">
    <div class="container">
      <div class="room-controls">
        <div class="create-room">
          <input 
            v-model="newRoomName" 
            placeholder="Название новой комнаты"
            @keyup.enter="createRoom"
            maxlength="50"
          >
          <button @click="createRoom" class="btn-primary" :disabled="isCreating">
            {{ isCreating ? 'Создание...' : 'Создать комнату' }}
          </button>
        </div>
      </div>
      <div class="rooms-grid">
        <div v-if="loading" class="loading-state">
          <div class="spinner"></div>
          <p>Загрузка комнат...</p>
        </div>
        
        <div v-else-if="rooms.length === 0" class="empty-state">
          <div class="empty-icon">🏠</div>
          <h3>Нет доступных комнат</h3>
          <p>Создайте первую комнату для начала общения</p>
        </div>
        <div 
          v-else 
          v-for="room in rooms" 
          :key="room.id"
          class="room-card"
          :class='is-active'
          @click="joinRoom(room.id)"
        >
          <div class="room-card-content">
            <div class="room-icon">
              {{ room.isFull ? '🔒' : '🚪' }}
            </div>
            <div class="room-details">
              <h3 class="room-name">{{ room.name }}</h3>
              <div class="room-meta">
                <span class="users-count">
                </span>
              </div>
            </div>
            <button 
              class="btn-join"
              @click.stop="joinRoom(room.id)"
            >
              {{ 'Войти' }}
            </button>
          </div>
        </div>
      </div>
      <div v-if="error" class="error-message">{{ error }}</div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, inject } from 'vue';
import { useRouter } from 'vue-router';
import { connect_websocket } from '../scripts/index.js';
import { HttpClient } from '../scripts/http_client.js'

const router = useRouter();
const http_client = new HttpClient();
const rooms = ref([]);
const loading = ref(false);
const isCreating = ref(false);
const error = ref('');
const newRoomName = ref('');
let pollingInterval = null;

// Загрузка списка комнат
const fetchRooms = async () => {
  if (loading.value) return;
  try {
    loading.value = true;
    rooms.value = await http_client.get_rooms();
    error.value = '';
  } catch (err) {
    console.error('Ошибка загрузки комнат:', err);
    error.value = 'Не удалось загрузить список комнат';
  } finally {
    loading.value = false;
  }
};

// Создание комнаты
const createRoom = async () => {
  const name = newRoomName.value.trim();
  if (!name) {
    error.value = 'Введите название комнаты';
    return;
  }
  if (isCreating.value) return;
  try {
    isCreating.value = true;
    error.value = '';
    const room = await http_client.create_room(name);
    newRoomName.value = '';
    router.push(`/room/${room.id}`);
  } catch (err) {
    error.value = err.message || 'Ошибка создания комнаты';
  } finally {
    isCreating.value = false;
  }
};

// Вход в комнату
const joinRoom = (room_id) => {
  router.push(`/room/${room_id}`);
};

// Поллинг комнат
const startPolling = () => {
  if (pollingInterval) return;
  pollingInterval = setInterval(fetchRooms, 5000);
};

const stopPolling = () => {
  if (pollingInterval) {
    clearInterval(pollingInterval);
    pollingInterval = null;
  }
};

onMounted(() => {
  fetchRooms();
  startPolling();
});

onUnmounted(() => {
  stopPolling();
});
</script>
<style src="../styles/menu.css" scoped></style>