import amqp, {Connection, Channel, ConsumeMessage, ChannelModel} from "amqplib";
import { IProvider } from "../../types/providers/IProvider";
import { Message } from "../../types/Message";
import { v4 as uuidv4 } from 'uuid';

type PendingHandler = {
    resolve: (value: any) => void;
    reject: (reason?: any) => void;
    timer: NodeJS.Timeout;
};

export class RabbitMQProvider implements IProvider {
    private connection!: ChannelModel;
    private channel!: Channel;
    private clientId!: string;
    public enableLog: boolean = true;

    // responseQueue -> (correlationId -> handler)
    private pendingRequests: Map<string, Map<string, PendingHandler>> = new Map();

    constructor(
        private readonly url: string,        // amqp://login:pass@host:port
    ) {
        this.clientId = `fc-${uuidv4()}`
    }

    async connect(): Promise<void> {
        this.connection = await amqp.connect(this.url);
        this.channel = await this.connection.createChannel();

        if (this.enableLog) console.log(`[RabbitMQ] Connected to ${this.url}`);
    }

    async disconnect(): Promise<void> {
        if (this.channel) await this.channel.close();
        if (this.connection) await this.connection.close();
        if (this.enableLog) console.log("[RabbitMQ] Disconnected");
    }

    async publish(topic: string, message: Message): Promise<void> {
        if (!this.channel) {
            if (this.enableLog) console.error("RabbitMQ channel not initialized");
            return;
        }
        await this.channel.assertQueue(topic, { durable: false });
        this.channel.sendToQueue(topic, Buffer.from(JSON.stringify(message)));
    }

    async subscribe(
        topic: string,
        handler: (msg: Message, raw: ConsumeMessage) => Promise<void> | void
    ): Promise<void> {
        if (!this.channel) {
            if (this.enableLog) console.error("RabbitMQ channel not initialized");
            return;
        }

        await this.channel.assertQueue(topic, { durable: false });
        await this.channel.consume(topic, async (msg) => {
            if (!msg) return;
            try {
                const decoded = JSON.parse(msg.content.toString()) as Message;
                await handler(decoded, msg);
                this.channel.ack(msg);
            } catch (err) {
                if (this.enableLog) console.error(`[RabbitMQ] Error handling message on ${topic}:`, err);
                this.channel.nack(msg, false, false);
            }
        });

        if (this.enableLog) console.log(`[RabbitMQ] Subscribed to ${topic}`);
    }

    async reply(args: { topic: string; message: Message }): Promise<void> {
        const responseTopic = `response_${args.topic}`;
        await this.publish(responseTopic, args.message);
    }

    async makeRequest(
        topic: string,
        message: Message,
        timeout = 5000
    ): Promise<Message> {
        const correlationId = message.id;
        const responseQueue = `response_${this.clientId}_${topic}`;

        if (!this.pendingRequests.has(responseQueue)) {
            this.pendingRequests.set(responseQueue, new Map());

            await this.channel.assertQueue(responseQueue, { durable: false });

            await this.channel.consume(responseQueue, (msg) => {
                if (!msg) return;
                try {
                    const decoded = JSON.parse(msg.content.toString());
                    const correlationId = decoded.id;

                    const topicHandlers = this.pendingRequests.get(responseQueue);
                    if (!topicHandlers) return;

                    const handler = topicHandlers.get(correlationId);
                    if (handler) {
                        handler.resolve(decoded);
                        clearTimeout(handler.timer);
                        topicHandlers.delete(correlationId);

                        if (topicHandlers.size === 0) {
                            this.pendingRequests.delete(responseQueue);
                            if (this.enableLog)
                                console.log(`[RabbitMQ] No more pending handlers for ${responseQueue}`);
                        }
                    }
                    this.channel.ack(msg);
                } catch (err) {
                    if (this.enableLog) console.error(`[RabbitMQ] Error in response handler:`, err);
                    this.channel.nack(msg, false, false);
                }
            });

            if (this.enableLog) console.log(`[RabbitMQ] Subscribed to response queue ${responseQueue}`);
        }

        return new Promise<Message>(async (resolve, reject) => {
            const timer = setTimeout(() => {
                const topicHandlers = this.pendingRequests.get(responseQueue);
                if (topicHandlers) topicHandlers.delete(correlationId);
                reject(new Error(`RabbitMQ request timed out for ${topic}`));
            }, timeout);

            this.pendingRequests.get(responseQueue)!.set(correlationId, { resolve, reject, timer });

            await this.publish(topic, message);
        });
    }

    setEnableLog(enable: boolean): void {
        this.enableLog = enable;
    }
}
