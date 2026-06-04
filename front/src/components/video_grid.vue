<template>
  <div v-if="users.length === 0" class="empty-state">
    <div class="empty-state__content">
      <div class="empty-state__icon">🎥</div>
      <h3>Никого нет в комнате</h3>
      <p>Нажмите «Войти», чтобы начать трансляцию</p>
    </div>
  </div>

  <div v-else class="video-grid">
    <VideoCard 
      v-for="peer in users" 
      :key="peer.id"
      :peer="peer"
      @set-video-src="$emit('set-video-src', $event, peer.stream)"
    />
  </div>
</template>

<script setup>
import VideoCard from './video_card.vue';

defineProps({
  users: {
    type: Array,
    required: true
  }
});

defineEmits(['set-video-src']);
</script>

<style src="../styles/video-grid.css" scoped></style>