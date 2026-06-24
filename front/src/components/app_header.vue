<template>
  <header class="header">
    <div class="header-container">
      <div class="header-left">
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
        <div v-if="isInRoom && roomName" class="room-name-badge">
          <span class="room-name-icon">🏠</span>
          <span class="room-name-text">{{ roomName }}</span>
        </div>
      </div>
    </div>
  </header>
</template>

<script setup>
import { computed } from 'vue';

const props = defineProps({
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

</script>
<style src="../styles/header.css" scoped></style>
