import amqp, {Connection, Channel, ConsumeMessage, ChannelModel} from "amqplib";
import { IProvider } from "../../types/providers/IProvider";
import { Message } from "../../types/Message";
import { PendingHandler } from "../../types/PendingHandler";
import {v4 as uuidv4} from "uuid";

export class RabbitMQProvider implements IProvider {
    private connection!: ChannelModel;
    private channel!: Channel;
    public enableLog: boolean = true;
    private responseQueue: string;
    private clientId: string;
    private wasResponseQueueCreated: boolean = false;

    // (correlationId -> handler)
    private pendingRequests: Map<string, PendingHandler> = new Map();

    // topic -> handler
    private topicHandlers: Map<string, (msg: Message, raw: ConsumeMessage) => Promise<void> | void> = new Map();

    constructor(
        private readonly url: string,     // пример: amqp://guest:guest@localhost:5672
    ) {
        this.clientId = `fc-${uuidv4()}`
        this.responseQueue = `response_${this.clientId}`;
    }

    async ready(): Promise<void> {}

    async connect(): Promise<void> {
        this.connection = await amqp.connect(this.url);
        this.channel = await this.connection.createChannel();

        // Ограничим нагрузку на одного потребителя
        this.channel.prefetch(10);

        if (!this.wasResponseQueueCreated) await this.channel.assertQueue(this.responseQueue, { durable: false });
        this.wasResponseQueueCreated = true;
        await this.channel.consume(
            this.responseQueue,
            (rawMsg) => {
                if (!rawMsg) return;
                const decoded = JSON.parse(rawMsg.content.toString()) as Message;
                if (this.enableLog) console.log(`[RabbitMQ] Received response:`, decoded);
                const corrId = decoded?.id;

                if (corrId && this.pendingRequests.has(corrId)) {
                    const handler = this.pendingRequests.get(corrId)!;
                    handler.resolve(decoded);
                    clearTimeout(handler.timer);
                    this.pendingRequests.delete(corrId);
                }
            },
            { noAck: true } // для reply-to не нужен ack
        );

        if (this.enableLog) console.log(`[RabbitMQ] Connected: ${this.url}`);
    }

    async disconnect(): Promise<void> {
        if (this.channel) {
            await this.channel.close();
        }
        if (this.connection) {
            await this.connection.close();
        }
        if (this.enableLog) console.log("[RabbitMQ] Disconnected");
    }

    async publish(topic: string, message: Message): Promise<void> {
        if (!this.channel) {
            if (this.enableLog) console.error("RabbitMQ channel not initialized");
            return;
        }
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
        this.topicHandlers.set(topic, handler);

        await this.channel.consume(topic, async (rawMsg) => {
            if (!rawMsg) return;
            try {
                const decoded = JSON.parse(rawMsg.content.toString()) as Message;
                await handler(decoded, rawMsg);
                // this.channel.ack(rawMsg);
            } catch (err) {
                if (this.enableLog) console.error("[RabbitMQ] Error in subscription handler:", err);
            }
        }, { noAck: true });
    }

    async reply(args: { topic: string; message: Message }): Promise<void> {
        const responseQueue = args.message?.metadata?.replyTo;
        if (this.enableLog) console.log(`[RabbitMQ] Replying to ${args.topic} with message:`, args.message);
        if (!responseQueue) {
            throw new Error("ReplyTo queue not found in message metadata");
        }

        this.channel.sendToQueue(
            responseQueue,
            Buffer.from(JSON.stringify(args.message)),
            { correlationId: args.message.id }
        );
    }

    async makeRequest(topic: string, message: Message, timeout = 5000): Promise<Message> {
        const correlationId = message.id;
        message = {...message, metadata: {...message.metadata, replyTo: this.responseQueue}};

        return new Promise<Message>((resolve, reject) => {
            const timer = setTimeout(() => {
                this.pendingRequests.delete(correlationId);
                reject(new Error(`RabbitMQ request timed out for ${topic}`));
            }, timeout);

            this.pendingRequests.set(correlationId, { resolve, reject, timer });
            if (this.enableLog) console.log(`[RabbitMQ] Sending request to ${topic} with content`, message);
            this.publish(topic, message);
        });
    }

    setEnableLog(enable: boolean): void {
        this.enableLog = enable;
    }
}
