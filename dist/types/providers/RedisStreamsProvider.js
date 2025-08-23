"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.RedisStreamsProvider = void 0;
const ioredis_1 = __importDefault(require("ioredis"));
const uuid_1 = require("uuid");
class RedisStreamsProvider {
    constructor(host, port, groupId, // consumer group
    consumerId // consumer name inside group
    ) {
        this.host = host;
        this.port = port;
        this.groupId = groupId;
        this.consumerId = consumerId;
        this.enableLog = true;
        // responseTopic -> (correlationId -> handler)
        this.pendingRequests = new Map();
        this.isConsuming = false;
        this.clientId = `fc-${(0, uuid_1.v4)()}`;
    }
    async connect() {
        this.producer = new ioredis_1.default({ host: this.host, port: this.port });
        this.consumer = new ioredis_1.default({ host: this.host, port: this.port });
        if (this.enableLog)
            console.log(`[Redis] Connected to ${this.host}:${this.port}`);
    }
    async disconnect() {
        await this.producer.quit();
        await this.consumer.quit();
        if (this.enableLog)
            console.log("[Redis] Disconnected");
    }
    async publish(topic, message) {
        await this.producer.xadd(topic, "*", "value", JSON.stringify(message));
    }
    async subscribe(topic, handler) {
        try {
            // Создаем группу, если нет
            await this.producer.xgroup("CREATE", topic, this.groupId, "$", "MKSTREAM").catch(() => { });
            if (!this.isConsuming) {
                this.isConsuming = true;
                this.consumeLoop(topic, handler);
            }
        }
        catch (err) {
            if (this.enableLog)
                console.error(`[Redis] Subscribe error:`, err);
        }
    }
    async consumeLoop(topic, handler) {
        while (this.isConsuming) {
            try {
                const streams = await this.consumer.call("XREADGROUP", "GROUP", this.groupId, this.consumerId, "BLOCK", "5000", "COUNT", "10", "STREAMS", topic, ">");
                if (streams) {
                    for (const [, messages] of streams) {
                        for (const [id, fields] of messages) {
                            try {
                                const rawValue = fields[1];
                                const decoded = JSON.parse(rawValue);
                                await handler(decoded, { id, fields });
                                await this.consumer.xack(topic, this.groupId, id);
                            }
                            catch (err) {
                                if (this.enableLog)
                                    console.error(`[Redis] Error handling message on ${topic}:`, err);
                            }
                        }
                    }
                }
            }
            catch (err) {
                if (this.enableLog)
                    console.error(`[Redis] Consumer loop error:`, err);
            }
        }
    }
    async reply(args) {
        const responseTopic = `response_${args.topic}`;
        await this.publish(responseTopic, args.message);
    }
    async makeRequest(topic, message, timeout = 5000) {
        const correlationId = message.id;
        const responseTopic = `response_${this.clientId}_${topic}`;
        if (!this.pendingRequests.has(responseTopic)) {
            this.pendingRequests.set(responseTopic, new Map());
            // Подписка на ответный стрим
            await this.producer.xgroup("CREATE", responseTopic, this.groupId, "$", "MKSTREAM").catch(() => { });
            this.consumeLoop(responseTopic, async (decoded) => {
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
                            console.log(`[Redis] No more pending handlers for ${responseTopic}`);
                    }
                }
            });
        }
        return new Promise(async (resolve, reject) => {
            const timer = setTimeout(() => {
                const topicHandlers = this.pendingRequests.get(responseTopic);
                if (topicHandlers)
                    topicHandlers.delete(correlationId);
                reject(new Error(`Redis request timed out for ${topic}`));
            }, timeout);
            this.pendingRequests.get(responseTopic).set(correlationId, { resolve, reject, timer });
            await this.publish(topic, message);
        });
    }
    setEnableLog(enable) {
        this.enableLog = enable;
    }
}
exports.RedisStreamsProvider = RedisStreamsProvider;
