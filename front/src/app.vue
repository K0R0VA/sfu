<template>
  <div class="app-wrapper">
    <AppHeader 
      :is-joined="isJoined"
      :is-connecting="isConnecting"
      :room-status="roomStatus"
      @join="joinRoom"
    />
    
    <main class="content-area">
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
import { useWebRTC } from './scripts/use_webrtc.js';

const roomStatus = ref("Подключение к медиа-серверу на акторах");
const isJoined = ref(false);
const isConnecting = ref(false);
const users = ref([]);

const { joinRoom: joinWebRTCRoom, setVideoSrc } = useWebRTC(users, isJoined, isConnecting, roomStatus);

const joinRoom = async () => {
  await joinWebRTCRoom();
};
</script>

<style src="./styles/main.css" scoped></style>