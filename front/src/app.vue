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
      <div>{{}}</div>
    </main>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue';
import AppHeader from './components/app_header.vue';
import VideoGrid from './components/video_grid.vue';
import { connect_websocket, setVideoSrc } from './scripts/index.js';


const roomStatus = ref("Готов к подключению");
const isJoined = ref(false);
const isConnecting = ref(false);
const error = ref('');
const users_map = ref(new Map());
const users = computed(() => {
  return Array.from(users_map.value.entries()).map(([id, data]) => ({
    id: id,
    stream: data.stream,
    isLocal: data.isLocal,
  }));
});

let userWebsocket;

const joinRoom = () => {
  userWebsocket = connect_websocket(users_map, isJoined, isConnecting, roomStatus);
};

const leaveRoom = () => {
  userWebsocket.disconnect();
}



</script>

<style src="./styles/main.css"></style>