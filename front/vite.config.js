import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  server: {
    // Разрешаем подключения со всех устройств в локалке (нужно для телефона)
    host: '0.0.0.0', 
    port: 5173
  }
})