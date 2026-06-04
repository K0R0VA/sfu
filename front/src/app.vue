<template>
  <div class="app">
    <AppHeader 
      :is-joined="isJoined"
      :is-connecting="isConnecting"
      :room-status="roomStatus"
      @join="joinRoom"
    />
    
    <main class="main-content">
      <VideoGrid 
        :users="users"
        @set-video-src="setVideoSrc"
      />
    </main>
  </div>
</template>

<script setup>
import { ref } from 'vue';
import AppHeader from './components/app_header.vue';
import VideoGrid from './components/video_grid.vue';
import { connect_websocket, setVideoSrc } from './scripts/index.js';

const roomStatus = ref("Готов к подключению");
const isJoined = ref(false);
const isConnecting = ref(false);
const users = ref([]);


const joinRoom = () => {
  connect_websocket(users, isJoined, isConnecting, roomStatus);
};
</script>

<style src="./styles/main.css"></style>