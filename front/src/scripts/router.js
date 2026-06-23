import { createRouter, createWebHistory } from 'vue-router';
import RoomList from '../components/menu.vue';
import RoomView from '../components/room.vue';

const routes = [
  {
    path: '/',
    name: 'RoomList',
    component: RoomList,
    meta: { title: 'Выбор комнаты' }
  },
  {
    path: '/room/:room_id',
    name: 'Room',
    component: RoomView,
    props: true,
    meta: { title: 'Комната' }
  },
  {
    path: '/:pathMatch(.*)*',
    redirect: '/'
  }
];

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes
});

// Обновление заголовка страницы
router.beforeEach((to, from, next) => {
  document.title = to.meta.title || 'Видео чат';
  next();
});

export default router;