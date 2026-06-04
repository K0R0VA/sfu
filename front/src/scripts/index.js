import { ref } from 'vue';
import { UserWebsocket } from './websocket';

export function connect_websocket(users, isJoined, isConnecting, roomStatus) {
  isConnecting.value = true;
  roomStatus.value = "Запрос доступа к камере...";
  new UserWebsocket(users, roomStatus, isConnecting, isJoined)
}


export function setVideoSrc(el, stream) {
  if (el && stream && el.srcObject !== stream) {
    el.srcObject = stream;
    el.play().catch(e => console.error("Ошибка автоплея:", e));
  }
}