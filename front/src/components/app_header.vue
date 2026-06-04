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

      <div class="buttons">
        <button 
          v-if="!isJoined && !isConnecting"
          @click="$emit('join')"
          class="join-btn"
        >
          <span class="btn-icon">🎥</span>
          <span>Войти</span>
        </button>
        
        <button 
          v-if="isJoined"
          @click="$emit('leave')"
          class="leave-btn"
        >
          <span class="btn-icon">🚪</span>
          <span>Выйти</span>
        </button>
        
        <button 
          v-if="isConnecting"
          class="join-btn connecting"
          disabled
        >
          <div class="spinner"></div>
          <span>Подключение</span>
        </button>
      </div>
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

const emit = defineEmits(['join', 'leave']);

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