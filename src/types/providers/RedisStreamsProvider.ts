import Redis from "ioredis";
import {IProvider} from "../../types/providers/IProvider";
import {Message} from "../../types/Message";
import {v4 as uuidv4} from "uuid";

type PendingHandler = {
    resolve: (value: any) => void;
    reject: (reason?: any) => void;
    timer: NodeJS.Timeout;
};

export class RedisStreamsProvider implements IProvider {
    private producer!: Redis;
    private consumer!: Redis;
    private clientId!: string;
    public enableLog: boolean = true;
    private wasResponseQueueCreated: boolean = false;

    // (correlationId -> handler)
    private pendingRequests: Map<string, PendingHandler> = new Map();

    private isConsuming: boolean = false;
    private responseTopic: string;

    constructor(
        private readonly host: string,
        private readonly port: number,
        private readonly groupId: string,   // consumer group
        private readonly consumerId: string // consumer name inside group
    ) {
        this.clientId = `fc-${uuidv4()}`
        this.responseTopic = `response_${this.clientId}`;
    }

    async ready(): Promise<void> {
    }

    async connect(): Promise<void> {
        this.producer = new Redis({host: this.host, port: this.port});
        this.consumer = new Redis({host: this.host, port: this.port});

        if (this.enableLog) console.log(`[Redis] Connected to ${this.host}:${this.port}`);
    }

    async disconnect(): Promise<void> {
        await this.producer.quit();
        await this.consumer.quit();
        if (this.enableLog) console.log("[Redis] Disconnected");
    }

    async publish(topic: string, message: Message): Promise<void> {
        if (this.enableLog) console.log(`[RSProvider] Publishing to ${message?.metadata?.replyTo}`, message)
        await this.producer.xadd(topic, "*", "value", JSON.stringify(message));
    }

    async subscribe(
        topic: string,
        handler: (msg: Message, raw: any) => Promise<void> | void
    ): Promise<void> {
        try {
            // Создаем группу, если нет
            await this.producer.xgroup("CREATE", topic, this.groupId, "$", "MKSTREAM").catch(() => {
            });

            if (!this.isConsuming) {
                this.isConsuming = true;
                this.consumeLoop(topic, handler);
            }
        } catch (err) {
            if (this.enableLog) console.error(`[Redis] Subscribe error:`, err);
        }
    }

    private async consumeLoop(topic: string, handler: (msg: Message, raw: any) => Promise<void> | void) {
        if (this.enableLog) console.log(`[RSProvider] Subscribing to ${topic}`)
        while (this.isConsuming) {
            try {
                const streams = await this.consumer.call(
                    "XREADGROUP",
                    "GROUP", this.groupId, this.consumerId,
                    "BLOCK", "5000",
                    "COUNT", "10",
                    "STREAMS", topic,
                    ">"
                ) as [string, [string, string[]][]][] | null;

                if (streams) {
                    for (const [, messages] of streams) {
                        for (const [id, fields] of messages) {
                            try {
                                const rawValue = fields[1];
                                const decoded = JSON.parse(rawValue) as Message;
                                if (this.enableLog) console.log(`[RSProvider] Got message by ${topic}`, decoded)
                                await handler(decoded, {id, fields});
                                await this.consumer.xack(topic, this.groupId, id);
                            } catch (err) {
                                if (this.enableLog) console.error(`[Redis] Error handling message on ${topic}:`, err);
                            }
                        }
                    }
                }
            } catch (err) {
                if (this.enableLog) console.error(`[Redis] Consumer loop error:`, err);
            }
        }
    }

    async reply(args: { topic: string, message: Message }): Promise<void> {
        const responseTopic = args.message?.metadata?.replyTo;
        if (this.enableLog) console.log(`[RSProvider] Replying to ${args.message?.metadata?.replyTo}`)
        if (!responseTopic) {
            throw new Error("Reply to topic not found in message metadata");
        }
        await this.publish(responseTopic, args.message);
    }

    async makeRequest(
        topic: string,
        message: Message,
        timeout = 5000
    ): Promise<Message> {
        const correlationId = message.id;
        message = {...message, metadata: {...message.metadata, replyTo: this.responseTopic}};

        if (!this.wasResponseQueueCreated) {
            // Подписка на ответный стрим
            await this.producer.xgroup("CREATE", this.responseTopic, this.groupId, "$", "MKSTREAM").catch(() => {
            });
            this.consumeLoop(this.responseTopic, async (decoded) => {
                const correlationId = decoded.id;

                const handler = this.pendingRequests.get(correlationId);
                if (handler) {
                    handler.resolve(decoded);
                    clearTimeout(handler.timer);
                    this.pendingRequests.delete(correlationId);
                }
            });
            this.wasResponseQueueCreated = true;
        }



        return new Promise<Message>(async (resolve, reject) => {
            const timer = setTimeout(() => {
                if (this.pendingRequests.has(correlationId)) this.pendingRequests.delete(correlationId);
                reject(new Error(`Redis request timed out for ${topic}`));
            }, timeout);

            this.pendingRequests.set(correlationId, {resolve, reject, timer});


            await this.publish(topic, message);
        });
    }

    setEnableLog(enable: boolean): void {
        this.enableLog = enable;
    }
}
