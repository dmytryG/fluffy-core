import { Kafka, Producer, Consumer, EachMessagePayload, logLevel } from "kafkajs";
import { IProvider } from "../../types/providers/IProvider";
import {Message} from "../../types/Message";

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

    constructor(
        private readonly brokers: string[],   // список брокеров
        private readonly clientId: string,    // идентификатор клиента
        private readonly groupId: string      // groupId для consumer
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
            messages: [
                { value: JSON.stringify(message) }
            ],
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

        await this.consumer.subscribe({ topic, fromBeginning: false });

        await this.consumer.run({
            eachMessage: async (payload: EachMessagePayload) => {
                try {
                    const decoded = JSON.parse(payload.message.value?.toString() || "{}") as Message;
                    await handler(decoded, payload);
                } catch (err) {
                    if (this.enableLog) console.error(`[Kafka] Error handling message on ${topic}:`, err);
                }
            },
        });
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
        const responseTopic = `response_${topic}`;

        if (!this.pendingRequests.has(responseTopic)) {
            this.pendingRequests.set(responseTopic, new Map());

            // Подписываемся на responseTopic первый раз
            await this.consumer.subscribe({ topic: responseTopic, fromBeginning: false });

            await this.consumer.run({
                eachMessage: async (payload: EachMessagePayload) => {
                    try {
                        const decoded = JSON.parse(payload.message.value?.toString() || "{}");
                        const correlationId = decoded.id

                        const topicHandlers = this.pendingRequests.get(responseTopic);
                        if (!topicHandlers) return;

                        const handler = topicHandlers.get(correlationId);
                        if (handler) {
                            handler.resolve(decoded);
                            clearTimeout(handler.timer);
                            topicHandlers.delete(correlationId);
                        }
                    } catch (err) {
                        if (this.enableLog) console.error(`[Kafka] Error in response handler:`, err);
                    }
                },
            });
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
