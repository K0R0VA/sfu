<template>
  <div class="app">
    <!-- Хедер всегда виден -->
    <AppHeader 
      :room-status="room_status"
      :room-name="room_name"
      @leave="handleLeave"
    />
    <main class="main-content">
      <router-view 
        :room-status="room_status"
        @leave-room="handleLeaveRoom"
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

// Предоставляем состояние дочерним компонентам
provide('roomState', {
  room_status,
  room_name,
});

const handleLeave = () => {
  router.push('/');
};

</script>