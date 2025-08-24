import { Kafka, Producer, Consumer, EachMessagePayload, logLevel } from "kafkajs";
import { IProvider } from "../../types/providers/IProvider";
import { Message } from "../../types/Message";

type PendingHandler = {
    resolve: (value: any) => void;
    reject: (reason?: any) => void;
    timer: NodeJS.Timeout;
};

export class KafkaProviderV2 implements IProvider {
    private kafka: Kafka;
    private producer!: Producer;
    private consumer!: Consumer;
    public enableLog: boolean = true;

    // responseTopic -> (correlationId -> handler)
    private pendingRequests: Map<string, Map<string, PendingHandler>> = new Map();

    // обычные подписки: topic -> handler
    private topicHandlers: Map<string, (msg: Message, raw: EachMessagePayload) => Promise<void> | void> = new Map();

    constructor(
        private readonly brokers: string[],
        private readonly clientId: string,
        private readonly groupId: string
    ) {
        this.kafka = new Kafka({
            clientId: this.clientId,
            brokers: this.brokers,
            logLevel: this.enableLog ? logLevel.INFO : logLevel.NOTHING,
        });
    }

    async connect(): Promise<void> {
        this.producer = this.kafka.producer();
        this.consumer = this.kafka.consumer({ groupId: this.groupId });

        await this.producer.connect();
        await this.consumer.connect();

        // единый consumer.run
        await this.consumer.run({
            autoCommit: false, // в RPC топиках коммиты не нужны
            eachMessage: async (payload: EachMessagePayload) => {
                try {
                    const decoded = JSON.parse(payload.message.value?.toString() || "{}") as Message;
                    const topic = payload.topic;

                    // 1. Обработка RPC response
                    const pending = this.pendingRequests.get(topic);
                    if (pending) {
                        const handler = pending.get(decoded.id);
                        if (handler) {
                            handler.resolve(decoded);
                            clearTimeout(handler.timer);
                            pending.delete(decoded.id);
                            return;
                        }
                    }

                    // 2. Обычный подписанный хендлер
                    const handler = this.topicHandlers.get(topic);
                    if (handler) {
                        await handler(decoded, payload);
                    }
                } catch (err) {
                    if (this.enableLog) console.error(`[Kafka] Error in message handler:`, err);
                }
            },
        });

        if (this.enableLog) console.log(`[Kafka] Connected to brokers: ${this.brokers.join(", ")}`);
    }

    async disconnect(): Promise<void> {
        if (this.producer) {
            await this.producer.disconnect();
        }
        if (this.consumer) {
            await this.consumer.disconnect();
        }
        if (this.enableLog) console.log("[Kafka] Disconnected");
    }

    async publish(topic: string, message: Message): Promise<void> {
        if (!this.producer) {
            if (this.enableLog) console.error("Kafka producer not initialized");
            return;
        }

        await this.producer.send({
            topic,
            messages: [{ value: JSON.stringify(message) }],
        });
    }

    async subscribe(
        topic: string,
        handler: (msg: Message, raw: EachMessagePayload) => Promise<void> | void
    ): Promise<void> {
        if (!this.consumer) {
            if (this.enableLog) console.error("Kafka consumer not initialized");
            return;
        }

        this.topicHandlers.set(topic, handler);
        await this.consumer.subscribe({ topic, fromBeginning: false });
    }

    async reply(args: { topic: string; message: Message }): Promise<void> {
        const responseTopic = `response_${args.topic}`;
        await this.publish(responseTopic, args.message);
    }

    async makeRequest(topic: string, message: Message, timeout = 5000): Promise<Message> {
        const correlationId = message.id;
        const responseTopic = `response_${topic}`;

        // подписываемся на responseTopic, если еще нет
        if (!this.pendingRequests.has(responseTopic)) {
            this.pendingRequests.set(responseTopic, new Map());
            await this.consumer.subscribe({ topic: responseTopic, fromBeginning: false });
        }

        return new Promise<Message>(async (resolve, reject) => {
            const timer = setTimeout(() => {
                const topicHandlers = this.pendingRequests.get(responseTopic);
                if (topicHandlers) topicHandlers.delete(correlationId);
                reject(new Error(`Kafka request timed out for ${topic}`));
            }, timeout);

            this.pendingRequests.get(responseTopic)!.set(correlationId, { resolve, reject, timer });

            // Отправляем запрос
            await this.publish(topic, message);
        });
    }

    setEnableLog(enable: boolean): void {
        this.enableLog = enable;
    }
}
