export class SmartWebSocket {
    constructor(url) {
        this.url = url;
        this.reconnectAttempts = 0;
        this.maxReconnectInterval = 10000; // Максимум 10 секунд между попытками
        this.isAlive = false;
        this.delayed_messages = [];
        this.connect();
    }

    connect() {
        console.log("Подключение к WebSocket...");
        this.ws = new WebSocket(this.url);

        this.ws.onopen = () => {
            console.log("WebSocket успешно подключен!");
            if (!this.isAlive) {
                this.delayed_messages.forEach((m) => this.ws.send(m));
                this.delayed_messages = [];
            }
            this.reconnectAttempts = 0; // Сбрасываем счетчик попыток
            this.isAlive = true;
        };

        this.ws.onclose = (e) => {
            this.isAlive = false;
            console.log(`WebSocket закрыт (код: ${e.code}). Запуск реконнекта...`);
            this.reconnect();
        };

        this.ws.onerror = (err) => {
            console.error("Ошибка WebSocket:", err);
            this.ws.close(); // Триггерит onclose, где сработает реконнект
        };
    }

    reconnect() {
        if (this.isAlive) return;

        this.reconnectAttempts++;
        // Экспоненциальная задержка: 1с, 2с, 4с, 8с, далее каждые 10с
        let delay = Math.min(Math.pow(2, this.reconnectAttempts) * 1000, this.maxReconnectInterval);
        
        console.log(`Следующая попытка подключения через ${delay / 1000} сек...`);
        setTimeout(() => {
            this.connect();
        }, delay);
    }

    // Прослойка для отправки сообщений
    send(data) {
        if (this.isAlive) {
            this.ws.send(data);
        }
        else {
            this.delayed_messages.push(data);
        }
    }
    on_message(f) {
        this.ws.onmessage = f;
    }
    close() {
        this.ws.close();
        this.ws = null;
    }
}