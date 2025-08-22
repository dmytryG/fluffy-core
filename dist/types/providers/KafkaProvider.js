"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaProvider = void 0;
const kafkajs_1 = require("kafkajs");
class KafkaProvider {
    constructor(brokers, // список брокеров
    clientId, // идентификатор клиента
    groupId // groupId для consumer
    ) {
        this.brokers = brokers;
        this.clientId = clientId;
        this.groupId = groupId;
        this.enableLog = true;
        // responseTopic -> (correlationId -> handler)
        this.pendingRequests = new Map();
        this.kafka = new kafkajs_1.Kafka({
            clientId: this.clientId,
            brokers: this.brokers,
            logLevel: this.enableLog ? kafkajs_1.logLevel.INFO : kafkajs_1.logLevel.NOTHING,
        });
    }
    async connect() {
        this.producer = this.kafka.producer();
        this.consumer = this.kafka.consumer({ groupId: this.groupId });
        await this.producer.connect();
        await this.consumer.connect();
        if (this.enableLog)
            console.log(`[Kafka] Connected to brokers: ${this.brokers.join(", ")}`);
    }
    async disconnect() {
        if (this.producer) {
            await this.producer.disconnect();
        }
        if (this.consumer) {
            await this.consumer.disconnect();
        }
        if (this.enableLog)
            console.log("[Kafka] Disconnected");
    }
    async publish(topic, message) {
        if (!this.producer) {
            if (this.enableLog)
                console.error("Kafka producer not initialized");
            return;
        }
        await this.producer.send({
            topic,
            messages: [
                { value: JSON.stringify(message) }
            ],
        });
    }
    async subscribe(topic, handler) {
        if (!this.consumer) {
            if (this.enableLog)
                console.error("Kafka consumer not initialized");
            return;
        }
        await this.consumer.subscribe({ topic, fromBeginning: false });
        await this.consumer.run({
            eachMessage: async (payload) => {
                try {
                    const decoded = JSON.parse(payload.message.value?.toString() || "{}");
                    await handler(decoded, payload);
                }
                catch (err) {
                    if (this.enableLog)
                        console.error(`[Kafka] Error handling message on ${topic}:`, err);
                }
            },
        });
    }
    async reply(args) {
        const responseTopic = `response_${args.topic}`;
        await this.publish(responseTopic, args.message);
    }
    async makeRequest(topic, message, timeout = 5000) {
        const correlationId = message.id;
        const responseTopic = `response_${topic}`;
        if (!this.pendingRequests.has(responseTopic)) {
            this.pendingRequests.set(responseTopic, new Map());
            // Подписываемся на responseTopic первый раз
            await this.consumer.subscribe({ topic: responseTopic, fromBeginning: false });
            await this.consumer.run({
                eachMessage: async (payload) => {
                    try {
                        const decoded = JSON.parse(payload.message.value?.toString() || "{}");
                        const correlationId = decoded.id;
                        const topicHandlers = this.pendingRequests.get(responseTopic);
                        if (!topicHandlers)
                            return;
                        const handler = topicHandlers.get(correlationId);
                        if (handler) {
                            handler.resolve(decoded);
                            clearTimeout(handler.timer);
                            topicHandlers.delete(correlationId);
                            if (topicHandlers.size === 0) {
                                this.pendingRequests.delete(responseTopic);
                                if (this.enableLog)
                                    console.log(`[Kafka] No more pending handlers for ${responseTopic}`);
                                // тут можно вызвать unsubscribe, но у KafkaJS нет удобного метода
                            }
                        }
                    }
                    catch (err) {
                        if (this.enableLog)
                            console.error(`[Kafka] Error in response handler:`, err);
                    }
                },
            });
        }
        return new Promise(async (resolve, reject) => {
            const timer = setTimeout(() => {
                const topicHandlers = this.pendingRequests.get(responseTopic);
                if (topicHandlers)
                    topicHandlers.delete(correlationId);
                reject(new Error(`Kafka request timed out for ${topic}`));
            }, timeout);
            this.pendingRequests.get(responseTopic).set(correlationId, { resolve, reject, timer });
            // Отправляем запрос
            await this.publish(topic, message);
        });
    }
    setEnableLog(enable) {
        this.enableLog = enable;
    }
}
exports.KafkaProvider = KafkaProvider;
