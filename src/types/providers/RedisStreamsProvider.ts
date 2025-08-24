import Redis from "ioredis";
import { IProvider } from "../../types/providers/IProvider";
import { Message } from "../../types/Message";
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

    // responseTopic -> (correlationId -> handler)
    private pendingRequests: Map<string, Map<string, PendingHandler>> = new Map();

    private isConsuming: boolean = false;

    constructor(
        private readonly host: string,
        private readonly port: number,
        private readonly groupId: string,   // consumer group
        private readonly consumerId: string // consumer name inside group
    ) {
        this.clientId = `fc-${uuidv4()}`
    }

    async ready(): Promise<void> {}

    async connect(): Promise<void> {
        this.producer = new Redis({ host: this.host, port: this.port });
        this.consumer = new Redis({ host: this.host, port: this.port });

        if (this.enableLog) console.log(`[Redis] Connected to ${this.host}:${this.port}`);
    }

    async disconnect(): Promise<void> {
        await this.producer.quit();
        await this.consumer.quit();
        if (this.enableLog) console.log("[Redis] Disconnected");
    }

    async publish(topic: string, message: Message): Promise<void> {
        await this.producer.xadd(topic, "*", "value", JSON.stringify(message));
    }

    async subscribe(
        topic: string,
        handler: (msg: Message, raw: any) => Promise<void> | void
    ): Promise<void> {
        try {
            // Создаем группу, если нет
            await this.producer.xgroup("CREATE", topic, this.groupId, "$", "MKSTREAM").catch(() => {});

            if (!this.isConsuming) {
                this.isConsuming = true;
                this.consumeLoop(topic, handler);
            }
        } catch (err) {
            if (this.enableLog) console.error(`[Redis] Subscribe error:`, err);
        }
    }

    private async consumeLoop(topic: string, handler: (msg: Message, raw: any) => Promise<void> | void) {
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
                                await handler(decoded, { id, fields });
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

    async reply(args: {topic: string, message: Message}): Promise<void> {
        const responseTopic = `response_${args.topic}`;
        await this.publish(responseTopic, args.message);
    }

    async makeRequest(
        topic: string,
        message: Message,
        timeout = 5000
    ): Promise<Message> {
        const correlationId = message.id;
        const responseTopic = `response_${this.clientId}_${topic}`;

        if (!this.pendingRequests.has(responseTopic)) {
            this.pendingRequests.set(responseTopic, new Map());

            // Подписка на ответный стрим
            await this.producer.xgroup("CREATE", responseTopic, this.groupId, "$", "MKSTREAM").catch(() => {});
            this.consumeLoop(responseTopic, async (decoded) => {
                const correlationId = decoded.id;
                const topicHandlers = this.pendingRequests.get(responseTopic);
                if (!topicHandlers) return;

                const handler = topicHandlers.get(correlationId);
                if (handler) {
                    handler.resolve(decoded);
                    clearTimeout(handler.timer);
                    topicHandlers.delete(correlationId);
                    if (topicHandlers.size === 0) {
                        this.pendingRequests.delete(responseTopic);
                        if (this.enableLog) console.log(`[Redis] No more pending handlers for ${responseTopic}`);
                    }
                }
            });
        }

        return new Promise<Message>(async (resolve, reject) => {
            const timer = setTimeout(() => {
                const topicHandlers = this.pendingRequests.get(responseTopic);
                if (topicHandlers) topicHandlers.delete(correlationId);
                reject(new Error(`Redis request timed out for ${topic}`));
            }, timeout);

            this.pendingRequests.get(responseTopic)!.set(correlationId, { resolve, reject, timer });

            await this.publish(topic, message);
        });
    }

    setEnableLog(enable: boolean): void {
        this.enableLog = enable;
    }
}
