<template>
  <div class="app">
    <!-- Хедер всегда виден -->
    <AppHeader 
      :room-status="room_status"
      :room-name="room_name"
      :is-in-room="is_in_room"
      @leave="handleLeave"
    />
    <main class="main-content">
      <router-view 
        @leave-room="handleLeave"
      />
    </main>
  </div>
</template>

<script setup>
import { ref, computed, provide, onMounted, onUnmounted } from 'vue';
import { useRouter } from 'vue-router';
import AppHeader from './components/app_header.vue';

const router = useRouter();

// Глобальное состояние

const room_status = ref('Не в сети');
const room_name = ref('');
const is_in_room = ref(false);

// Предоставляем состояние дочерним компонентам
provide('roomState', {
  room_status,
  is_in_room,
  room_name,
});

const handleLeave = () => {
  router.push('/');
};

</script>
<style src="./styles/main.css" scoped></style>