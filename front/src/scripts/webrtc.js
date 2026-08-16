import { PublisherConnection } from "./publisher";
import { SubscriberConnection } from "./subscriber";

export class WebrtcConnection {
    constructor(users, ws) {
        this.subscriber = new SubscriberConnection(ws, users);
        this.publisher = new PublisherConnection(ws);
    }
    async add_ice_candidate(message) {
        switch (message.target) {
            case 'publisher': {
                await this.publisher.add_ice_candidate(message);
                break;
            }
            case 'subscriber': {
                await this.subscriber.add_ice_candidate(message);
                break;
            }
        }
    }
    async receive_answer(message) {
        switch (message.target) {
            case 'publisher': {
                await this.publisher.receive_answer(message);
                break;
            }
            case 'subscriber': {
                await this.subscriber.receive_answer(message);
                break;
            }
        }
    }
}