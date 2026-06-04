<template>
  <header class="main-header">
    <div class="logo-zone">
      <div 
        class="live-indicator" 
        :class="{ joined: isJoined, connecting: isConnecting }"
      ></div>
      <h1>Rust Media SFU Engine</h1>
    </div>
    <div class="status-badge">{{ roomStatus }}</div>
    <button 
      @click="$emit('join')"
      :disabled="isJoined || isConnecting" 
      class="action-btn"
      :class="{ active: isJoined }"
    >
      <span v-if="isConnecting" class="spinner"></span>
      {{ buttonText }}
    </button>
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
  if (props.isConnecting) return 'Присоединение...';
  if (props.isJoined) return 'Вы в комнате';
  return 'Войти в комнату';
});
</script>

<style src="../styles/header.css" scoped></style>