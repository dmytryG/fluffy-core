"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.RedisStreamsProvider = void 0;
const ioredis_1 = __importDefault(require("ioredis"));
const uuid_1 = require("uuid");
class RedisStreamsProvider {
    constructor(host, port, groupId) {
        this.host = host;
        this.port = port;
        this.groupId = groupId;
        this.enableLog = true;
        this.wasResponseQueueCreated = false;
        // (correlationId -> handler)
        this.pendingRequests = new Map();
        this.isConsuming = true;
        this.clientId = `fc-${(0, uuid_1.v4)()}`;
        this.responseTopic = `response_${this.clientId}`;
    }
    async ready() {
    }
    async connect() {
        this.producer = new ioredis_1.default({ host: this.host, port: this.port });
        this.consumer = new ioredis_1.default({ host: this.host, port: this.port });
        if (this.enableLog)
            console.log(`[Redis] Connected to ${this.host}:${this.port}`);
    }
    async disconnect() {
        this.isConsuming = false;
        await this.producer.quit();
        await this.consumer.quit();
        if (this.enableLog)
            console.log("[Redis] Disconnected");
    }
    async publish(topic, message) {
        if (this.enableLog)
            console.log(`[RSProvider] Publishing to ${topic}`, message);
        await this.producer.xadd(topic, "*", "value", JSON.stringify(message));
    }
    async subscribe(topic, handler) {
        try {
            // Создаем группу, если нет
            await this.producer.xgroup("CREATE", topic, this.groupId, "$", "MKSTREAM").catch(() => {
            });
            this.consumeLoop(topic, handler);
        }
        catch (err) {
            if (this.enableLog)
                console.error(`[Redis] Subscribe error:`, err);
        }
    }
    async consumeLoop(topic, handler) {
        if (this.enableLog)
            console.log(`[RSProvider] Subscribing to ${topic}`);
        let lastId = "0"; // читаем всё, начиная с начала
        while (this.isConsuming) {
            try {
                const streams = await this.consumer.xread(
                // @ts-ignore
                "BLOCK", 5000, "COUNT", 10, "STREAMS", topic, lastId === "0" ? "0" : lastId);
                if (streams) {
                    for (const [, messages] of streams) {
                        for (const [id, fields] of messages) {
                            const rawValue = fields[1];
                            const decoded = JSON.parse(rawValue);
                            if (this.enableLog)
                                console.log(`[RSProvider] Got message by ${topic}`, decoded);
                            await handler(decoded, { id, fields });
                            lastId = id;
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
        const responseTopic = args.message?.metadata?.replyTo;
        if (this.enableLog)
            console.log(`[RSProvider] Replying to ${args.message?.metadata?.replyTo}`);
        if (!responseTopic) {
            throw new Error("Reply to topic not found in message metadata");
        }
        await this.publish(responseTopic, args.message);
    }
    async makeRequest(topic, message, timeout = 5000) {
        const correlationId = message.id;
        message = { ...message, metadata: { ...message.metadata, replyTo: this.responseTopic } };
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
        return new Promise(async (resolve, reject) => {
            const timer = setTimeout(() => {
                if (this.pendingRequests.has(correlationId))
                    this.pendingRequests.delete(correlationId);
                reject(new Error(`Redis request timed out for ${topic}`));
            }, timeout);
            this.pendingRequests.set(correlationId, { resolve, reject, timer });
            await this.publish(topic, message);
        });
    }
    setEnableLog(enable) {
        this.enableLog = enable;
    }
}
exports.RedisStreamsProvider = RedisStreamsProvider;
