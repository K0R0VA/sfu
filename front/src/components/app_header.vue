<template>
  <header class="header">
    <div class="header-container">
      <div class="logo">
        <div class="logo-icon">
          <svg width="28" height="28" viewBox="0 0 24 24" fill="none">
            <path d="M12 2L2 7L12 12L22 7L12 2Z" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M2 17L12 22L22 17" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M2 12L12 17L22 12" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </div>
        <div class="logo-text">
          <span class="logo-title">Rust Media</span>
          <span class="logo-subtitle">SFU</span>
        </div>
      </div>

      <div class="status" :class="statusClass">
        <span class="status-dot"></span>
        <span class="status-text">{{ roomStatus }}</span>
      </div>

      <button 
        @click="$emit('join')"
        :disabled="isJoined || isConnecting" 
        class="join-btn"
        :class="{ 'joined': isJoined, 'connecting': isConnecting }"
      >
        <span v-if="!isConnecting && !isJoined" class="btn-icon">🎥</span>
        <div v-if="isConnecting" class="spinner"></div>
        <span v-if="isJoined" class="btn-icon">✓</span>
        <span>{{ buttonText }}</span>
      </button>
    </div>
  </header>
</template>

<script setup>
import { computed } from 'vue';

const props = defineProps({
  isJoined: Boolean,
  isConnecting: Boolean,
  roomStatus: String
});

const emit = defineEmits(['join']);

const buttonText = computed(() => {
  if (props.isConnecting) return 'Подключение';
  if (props.isJoined) return 'В комнате';
  return 'Войти';
});

const statusClass = computed(() => {
  if (props.isJoined) return 'status-online';
  if (props.isConnecting) return 'status-connecting';
  return 'status-offline';
});
</script>

<style src="../styles/header.css" scoped></style>