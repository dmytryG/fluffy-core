"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaProviderV2 = void 0;
const kafkajs_1 = require("kafkajs");
class KafkaProviderV2 {
    constructor(brokers, clientId, groupId) {
        this.brokers = brokers;
        this.clientId = clientId;
        this.groupId = groupId;
        this.enableLog = true;
        // responseTopic -> (correlationId -> handler)
        this.pendingRequests = new Map();
        // обычные подписки: topic -> handler
        this.topicHandlers = new Map();
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
        // единый consumer.run
        await this.consumer.run({
            autoCommit: false, // в RPC топиках коммиты не нужны
            eachMessage: async (payload) => {
                try {
                    const decoded = JSON.parse(payload.message.value?.toString() || "{}");
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
                }
                catch (err) {
                    if (this.enableLog)
                        console.error(`[Kafka] Error in message handler:`, err);
                }
            },
        });
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
            messages: [{ value: JSON.stringify(message) }],
        });
    }
    async subscribe(topic, handler) {
        if (!this.consumer) {
            if (this.enableLog)
                console.error("Kafka consumer not initialized");
            return;
        }
        this.topicHandlers.set(topic, handler);
        await this.consumer.subscribe({ topic, fromBeginning: false });
    }
    async reply(args) {
        const responseTopic = `response_${args.topic}`;
        await this.publish(responseTopic, args.message);
    }
    async makeRequest(topic, message, timeout = 5000) {
        const correlationId = message.id;
        const responseTopic = `response_${topic}`;
        // подписываемся на responseTopic, если еще нет
        if (!this.pendingRequests.has(responseTopic)) {
            this.pendingRequests.set(responseTopic, new Map());
            await this.consumer.subscribe({ topic: responseTopic, fromBeginning: false });
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
exports.KafkaProviderV2 = KafkaProviderV2;
