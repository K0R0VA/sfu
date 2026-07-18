<template>
  <div class="video-card" :class="{ 'video-card--local': peer.isLocal }">
    <div class="video-card__wrapper">
      <!-- ИССПРАВЛЕНО: Один единственный динамический ref, который делает сразу две задачи -->
      <video 
        :ref="(el) => { 
          videoElement = el; 
          if (el) emit('set-video-src', el, peer.stream); 
        }"
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
          <span class="badge-fps" v-if="fps > 0"> · {{ fps }} FPS</span>
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
const fps = ref(0); // Реактивная переменная для хранения текущего FPS
const videoElement = ref(null); // Ссылка на HTML5 Video элемент

let audioContext = null;
let analyser = null;
let source = null;
let animationId = null;
let fpsIntervalId = null; // ID интервала для подсчета кадров
let lastTotalFrames = 0; // Предыдущее количество декодированных кадров

// Функция замера реального FPS
const startFpsCounter = () => {
  if (fpsIntervalId) clearInterval(fpsIntervalId);
  lastTotalFrames = 0;
  fps.value = 0;

  fpsIntervalId = setInterval(() => {
    const video = videoElement.value;
    
    // Проверяем, поддерживает ли браузер современный API замера кадров
    if (video && video.getVideoPlaybackQuality) {
      const quality = video.getVideoPlaybackQuality();
      const totalFrames = quality.totalVideoFrames;
      
      // FPS — это разница между текущим числом кадров и числом кадров секунду назад
      if (lastTotalFrames > 0 && totalFrames >= lastTotalFrames) {
        fps.value = totalFrames - lastTotalFrames;
      } else if (totalFrames > 0 && lastTotalFrames === 0) {
        // Первая итерация, когда стрим только пошел
        fps.value = 25; // Дефолтное стартовое значение
      } else {
        fps.value = 0;
      }
      lastTotalFrames = totalFrames;
    } else {
      // Фолбэк для старых браузеров (Safari/iOS), еслиgetVideoPlaybackQuality не поддерживается
      if (video && video.webkitDecodedFrameCount) {
        const totalFrames = video.webkitDecodedFrameCount;
        if (lastTotalFrames > 0) {
          fps.value = totalFrames - lastTotalFrames;
        }
        lastTotalFrames = totalFrames;
      }
    }
  }, 1000); // Обновляем метрику строго раз в 1 секунду
};

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
  if (fpsIntervalId) {
    clearInterval(fpsIntervalId);
    fpsIntervalId = null;
  }
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
  fps.value = 0;
};

onMounted(() => {
  if (props.peer.stream) {
    if (!props.peer.isLocal) {
      setupAudioVisualization();
    }
    // Запускаем счетчик FPS для любого активного стрима
    startFpsCounter();
  }
});

watch(() => props.peer.stream, (newStream, oldStream) => {
  if (oldStream) {
    cleanup();
  }
  if (newStream) {
    nextTick(() => {
      if (!props.peer.isLocal) {
        setupAudioVisualization();
      }
      startFpsCounter();
    });
  }
});

onUnmounted(() => {
  cleanup();
});
</script>

<style src="../styles/video-card.css" scoped></style>