<template>
  <div class="video-card" :class="{ 'video-card--local': peer.isLocal }">
    <div class="video-card__wrapper">
      <video 
        :ref="(el) => emit('set-video-src', el, peer.stream)"
        autoplay 
        playsinline 
        webkit-playsinline
        :muted="peer.isLocal"
        class="video-card__player"
      ></video>
      
      <div class="video-card__overlay">
        <div class="video-card__badge" :class="{ 'badge-local': peer.isLocal }">
          <span class="badge-dot"></span>
          <span class="badge-text">{{ peer.isLocal ? 'Вы' : 'Участник' }}</span>
        </div>
        
        <div class="video-card__audio-indicator" v-if="!peer.isLocal && isAudioActive">
          <div class="audio-wave">
            <span></span><span></span><span></span><span></span>
          </div>
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
import { ref, onMounted, onUnmounted, watch, nextTick } from 'vue';

const props = defineProps({
  peer: {
    type: Object,
    required: true
  }
});

const emit = defineEmits(['set-video-src']);

const isAudioActive = ref(false);
let audioContext = null;
let analyser = null;
let source = null;
let animationId = null;

const setupAudioVisualization = async () => {
  if (props.peer.isLocal) return;
  if (!props.peer.stream) return;
  
  const audioTrack = props.peer.stream.getAudioTracks()[0];
  if (!audioTrack) return;
  
  try {
    audioContext = new (window.AudioContext || window.webkitAudioContext)();
    analyser = audioContext.createAnalyser();
    analyser.fftSize = 256;
    
    source = audioContext.createMediaStreamSource(props.peer.stream);
    source.connect(analyser);
    
    // Автоматически запускаем AudioContext при создании
    if (audioContext.state === 'suspended') {
      await audioContext.resume();
    }
    
    const dataArray = new Uint8Array(analyser.frequencyBinCount);
    
    const checkAudio = () => {
      if (!analyser || !props.peer.stream) {
        return;
      }
      
      analyser.getByteFrequencyData(dataArray);
      const average = dataArray.reduce((a, b) => a + b, 0) / dataArray.length;
      isAudioActive.value = average > 10;
      
      animationId = requestAnimationFrame(checkAudio);
    };
    
    checkAudio();
    
  } catch (error) {
    console.error('Ошибка визуализации звука:', error);
  }
};

const cleanup = () => {
  if (animationId) {
    cancelAnimationFrame(animationId);
    animationId = null;
  }
  if (source) {
    try {
      source.disconnect();
    } catch (e) {}
    source = null;
  }
  if (analyser) {
    try {
      analyser.disconnect();
    } catch (e) {}
    analyser = null;
  }
  if (audioContext) {
    audioContext.close().catch(console.error);
    audioContext = null;
  }
};

// При монтировании запускаем визуализацию
onMounted(() => {
  if (!props.peer.isLocal && props.peer.stream) {
    setupAudioVisualization();
  }
});

// Следим за изменением стрима
watch(() => props.peer.stream, (newStream, oldStream) => {
  if (oldStream) {
    cleanup();
  }
  if (newStream && !props.peer.isLocal) {
    nextTick(() => {
      setupAudioVisualization();
    });
  }
});

onUnmounted(() => {
  cleanup();
});
</script>

<style src="../styles/video-card.css" scoped></style>
