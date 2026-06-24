export class HttpClient { 
    constructor () {
        const serverIP = window.location.hostname;
        if (serverIP === 'localhost') {
            this.domain = `http://${serverIP}:8080/api`;
        } else {
            this.domain = `http://${serverIP}/api`;
        }
    }
    async get_rooms() {
        const response = await fetch(`${this.domain}/rooms`);
        if (!response.ok) throw new Error('Ошибка загрузки комнат');
        const rooms = await response.json();
        return rooms;
    }
    async create_room() {
        const response = await fetch(`${this.domain}/room`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name })
        });
        if (!response.ok) {
            const data = await response.text();
            throw new Error(data || 'Ошибка создания комнаты');
        }
        const room = await response.json();
        return room;
    }
}