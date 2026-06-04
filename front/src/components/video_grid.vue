<template>
  <div v-if="users.length === 0" class="empty-state">
    <div class="empty-icon">🎥</div>
    <h3>Комната пуста</h3>
    <p>Нажмите кнопку выше, чтобы запустить камеру и подключиться к акторам Rust.</p>
  </div>

  <div v-else class="video-grid">
    <VideoCard 
      v-for="peer in users" 
      :key="peer.id"
      :peer="peer"
      @set-video-src="(el) => emit('set-video-src', el, peer.stream)"
    />
  </div>
</template>

<script setup>
import VideoCard from './video_card.vue';

const props = defineProps({
  users: {
    type: Array,
    required: true
  }
});

const emit = defineEmits(['set-video-src']);
</script>

<style src="../styles/video-grid.css" scoped></style>