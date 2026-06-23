import { ref } from 'vue';
import { UserWebsocket } from './websocket';

export function connect_websocket(users, room_id, room_status, room_name) {
  room_status.value = "Запрос доступа к камере...";
  const ws = new UserWebsocket(users, room_id, room_status, room_name);
  ws.init();
  return ws;
}


export function setVideoSrc(el, stream) {
  if (el && stream && el.srcObject !== stream) {
    el.srcObject = stream;
    el.play().catch(e => console.error("Ошибка автоплея:", e));
  }
}