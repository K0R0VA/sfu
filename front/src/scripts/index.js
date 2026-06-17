import { ref } from 'vue';
import { UserWebsocket } from './websocket';

export function connect_websocket(users, isJoined, isConnecting, roomStatus, error) {
  isConnecting.value = true;
  roomStatus.value = "Запрос доступа к камере...";
  const ws = new UserWebsocket(users, roomStatus, isConnecting, isJoined, error);
  ws.init();
  return ws;
}


export function setVideoSrc(el, stream) {
  if (el && stream && el.srcObject !== stream) {
    el.srcObject = stream;
    el.play().catch(e => console.error("Ошибка автоплея:", e));
  }
}