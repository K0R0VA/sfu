<template>
  <div class="app">
    <AppHeader 
      :is-joined="isJoined"
      :is-connecting="isConnecting"
      :room-status="roomStatus"
      @join="joinRoom"
      @leave="leaveRoom"
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
let userWebsocket;

const joinRoom = () => {
  userWebsocket = connect_websocket(users, isJoined, isConnecting, roomStatus);
};

const leaveRoom = () => {
  userWebsocket.disconnect();
}

</script>

<style src="./styles/main.css"></style>