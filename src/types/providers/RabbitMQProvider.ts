import amqp, {Connection, Channel, ConsumeMessage, ChannelModel} from "amqplib";
import { IProvider } from "../../types/providers/IProvider";
import { Message } from "../../types/Message";

type PendingHandler = {
    resolve: (value: any) => void;
    reject: (reason?: any) => void;
    timer: NodeJS.Timeout;
};

export class RabbitMQProvider implements IProvider {
    private connection!: ChannelModel;
    private channel!: Channel;
    public enableLog: boolean = true;
    private responseQueue: string;

    // (correlationId -> handler)
    private pendingRequests: Map<string, PendingHandler> = new Map();

    // topic -> handler
    private topicHandlers: Map<string, (msg: Message, raw: ConsumeMessage) => Promise<void> | void> = new Map();

    constructor(
        private readonly url: string,     // пример: amqp://guest:guest@localhost:5672
        private readonly clientId: string
    ) {
        this.responseQueue = `response_${this.clientId}`;
    }

    async ready(): Promise<void> {}

    async connect(): Promise<void> {
        this.connection = await amqp.connect(this.url);
        this.channel = await this.connection.createChannel();

        // создаём очередь для RPC ответов
        await this.channel.assertQueue(this.responseQueue, { exclusive: true });

        // слушаем её
        await this.channel.consume(this.responseQueue, async (rawMsg) => {
            if (!rawMsg) return;
            try {
                const decoded = JSON.parse(rawMsg.content.toString()) as Message;
                const corrId = rawMsg.properties.correlationId;

                // 1. RPC response
                if (corrId && this.pendingRequests.has(corrId)) {
                    const handler = this.pendingRequests.get(corrId)!;
                    handler.resolve(decoded);
                    clearTimeout(handler.timer);
                    this.pendingRequests.delete(corrId);
                    this.channel.ack(rawMsg);
                    return;
                }

                // 2. Обычный хендлер по queue/topic
                const handler = this.topicHandlers.get(rawMsg.fields.routingKey);
                if (handler) {
                    await handler(decoded, rawMsg);
                }

                this.channel.ack(rawMsg);
            } catch (err) {
                if (this.enableLog) console.error("[RabbitMQ] Error in message handler:", err);
            }
        });

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
        this.topicHandlers.set(topic, handler);

        await this.channel.consume(topic, async (rawMsg) => {
            if (!rawMsg) return;
            try {
                const decoded = JSON.parse(rawMsg.content.toString()) as Message;
                await handler(decoded, rawMsg);
                this.channel.ack(rawMsg);
            } catch (err) {
                if (this.enableLog) console.error("[RabbitMQ] Error in subscription handler:", err);
            }
        });
    }

    async reply(args: { topic: string; message: Message }): Promise<void> {
        const responseQueue = args.message?.metadata?.replyTo;
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
        message = { ...message, metadata: { ...message.metadata, replyTo: this.responseQueue } };

        return new Promise<Message>(async (resolve, reject) => {
            const timer = setTimeout(() => {
                this.pendingRequests.delete(correlationId);
                reject(new Error(`RabbitMQ request timed out for ${topic}`));
            }, timeout);

            this.pendingRequests.set(correlationId, { resolve, reject, timer });

            await this.channel.assertQueue(topic, { durable: false });

            this.channel.sendToQueue(
                topic,
                Buffer.from(JSON.stringify(message)),
                {
                    correlationId,
                    replyTo: this.responseQueue,
                }
            );
        });
    }

    setEnableLog(enable: boolean): void {
        this.enableLog = enable;
    }
}
