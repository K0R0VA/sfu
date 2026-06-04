<template>
  <div 
    class="video-card"
    :class="{ 'local-card': peer.isLocal }"
  >
    <div class="video-wrapper">
      <video 
        :ref="(el) => emit('set-video-src', el, peer.stream)"
        autoplay 
        playsinline 
        webkit-playsinline
        :muted="true"
      ></video>
    </div>
    <div class="peer-meta">
      <span class="role-dot" :class="{ local: peer.isLocal }"></span>
      <span class="peer-id">{{ peer.isLocal ? 'Вы' : 'Сосед' }}: {{ peer.id.slice(0, 8) }}...</span>
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