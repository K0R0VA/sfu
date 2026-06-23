import { createApp } from 'vue';
import App from './app.vue';
import router from './scripts/router';

const app = createApp(App);
app.use(router);
app.mount('#app');