<template>
  <div class="video-card" :class="{ 'video-card--local': peer.isLocal }">
    <div class="video-card__wrapper">
      <video 
        :ref="(el) => emit('set-video-src', el, peer.stream)"
        autoplay 
        playsinline 
        webkit-playsinline
        :muted="true"
        class="video-card__player"
      ></video>
      
      <div class="video-card__overlay">
        <div class="video-card__badge" :class="{ 'badge-local': peer.isLocal }">
          <span class="badge-dot"></span>
          <span class="badge-text">{{ peer.isLocal ? 'Вы' : 'Участник' }}</span>
        </div>
        <div class="video-card__id">{{ peer.id.slice(0, 8) }}</div>
      </div>
      
      <div class="video-card__status" v-if="peer.isLocal">
        <div class="status-pulse"></div>
        <span>Трансляция</span>
      </div>
    </div>
  </div>
</template>

<script setup>
const props = defineProps({
  peer: {
    type: Object,
    required: true
  }
});

const emit = defineEmits(['set-video-src']);
</script>

<style src="../styles/video-card.css" scoped></style>
