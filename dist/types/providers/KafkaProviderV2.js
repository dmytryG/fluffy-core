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
        // (correlationId -> handler)
        this.pendingRequests = new Map();
        // обычные подписки: topic -> handler
        this.topicHandlers = new Map();
        this.kafka = new kafkajs_1.Kafka({
            clientId: this.clientId,
            brokers: this.brokers,
            logLevel: this.enableLog ? kafkajs_1.logLevel.INFO : kafkajs_1.logLevel.NOTHING,
        });
        this.responseTopic = `response_${this.clientId}`;
    }
    async ready() {
        // единый consumer.run
        await this.consumer.run({
            autoCommit: false, // в RPC топиках коммиты не нужны
            eachMessage: async (payload) => {
                try {
                    const decoded = JSON.parse(payload.message.value?.toString() || "{}");
                    const topic = payload.topic;
                    // 1. Обработка RPC response
                    const rpcHandler = this.pendingRequests.get(decoded.id);
                    if (rpcHandler) {
                        if (rpcHandler) {
                            rpcHandler.resolve(decoded);
                            clearTimeout(rpcHandler.timer);
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
    }
    async connect() {
        this.producer = this.kafka.producer();
        this.consumer = this.kafka.consumer({ groupId: this.groupId });
        await this.producer.connect();
        await this.consumer.connect();
        await this.consumer.subscribe({ topic: this.responseTopic, fromBeginning: false });
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
        return new Promise(async (resolve, reject) => {
            const timer = setTimeout(() => {
                this.pendingRequests.delete(correlationId);
                reject(new Error(`Kafka request timed out for ${topic}`));
            }, timeout);
            this.pendingRequests.set(correlationId, { resolve, reject, timer });
            // Отправляем запрос
            await this.publish(topic, message);
        });
    }
    setEnableLog(enable) {
        this.enableLog = enable;
    }
}
exports.KafkaProviderV2 = KafkaProviderV2;
