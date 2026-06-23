<template>
  <header class="header">
    <div class="header-container">
      <div class="header-left">
        <!-- Кнопка назад (показывается только в комнате) -->
        <button 
          v-if="isInRoom" 
          @click="$emit('back')" 
          class="back-btn"
          title="Вернуться к списку комнат"
        >
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M19 12H5M12 19l-7-7 7-7"/>
          </svg>
          <span>Назад</span>
        </button>

        <div class="logo" :class="{ 'with-back-btn': isInRoom }">
          <div class="logo-icon">
            <svg width="28" height="28" viewBox="0 0 24 24" fill="none">
              <path d="M12 2L2 7L12 12L22 7L12 2Z" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
              <path d="M2 17L12 22L22 17" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
              <path d="M2 12L12 17L22 12" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
          </div>
          <div class="logo-text">
            <span class="logo-title">Веб чат</span>
            <span class="logo-subtitle">SFU</span>
          </div>
        </div>
      </div>

      <div class="header-center">
        <div class="status" :class="statusClass">
          <span class="status-dot"></span>
          <span class="status-text">{{ roomStatus }}</span>
        </div>
        <div v-if="isInRoom && roomName" class="room-name-badge">
          <span class="room-name-icon">🏠</span>
          <span class="room-name-text">{{ roomName }}</span>
        </div>
      </div>

      <div class="header-right">
        <!-- Кнопка выхода (показывается только когда в комнате) -->
        <button 
          v-if="isJoined"
          @click="$emit('leave')"
          class="leave-btn"
        >
          <span class="btn-icon">🚪</span>
          <span>Выйти</span>
        </button>
        
        <!-- Индикатор подключения -->
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
  roomStatus: String,
  isInRoom: {
    type: Boolean,
    default: false
  },
  roomName: {
    type: String,
    default: ''
  }
});

const emit = defineEmits(['join', 'leave', 'back']);

const statusClass = computed(() => {
  if (props.isJoined) return 'status-online';
  if (props.isConnecting) return 'status-connecting';
  return 'status-offline';
});
</script>
<style src="../styles/header.css" scoped></style>
