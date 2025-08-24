"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.RabbitMQProvider = void 0;
const amqplib_1 = __importDefault(require("amqplib"));
const uuid_1 = require("uuid");
class RabbitMQProvider {
    constructor(url) {
        this.url = url;
        this.enableLog = true;
        // (correlationId -> handler)
        this.pendingRequests = new Map();
        // topic -> handler
        this.topicHandlers = new Map();
        this.clientId = `fc-${(0, uuid_1.v4)()}`;
        this.responseQueue = `response_${this.clientId}`;
    }
    async ready() { }
    async connect() {
        this.connection = await amqplib_1.default.connect(this.url);
        this.channel = await this.connection.createChannel();
        // создаём очередь для RPC ответов
        await this.channel.assertQueue(this.responseQueue, { exclusive: true });
        // слушаем её
        await this.channel.consume(this.responseQueue, async (rawMsg) => {
            if (!rawMsg)
                return;
            try {
                const decoded = JSON.parse(rawMsg.content.toString());
                const corrId = rawMsg.properties.correlationId;
                // 1. RPC response
                if (corrId && this.pendingRequests.has(corrId)) {
                    const handler = this.pendingRequests.get(corrId);
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
            }
            catch (err) {
                if (this.enableLog)
                    console.error("[RabbitMQ] Error in message handler:", err);
            }
        });
        if (this.enableLog)
            console.log(`[RabbitMQ] Connected: ${this.url}`);
    }
    async disconnect() {
        if (this.channel) {
            await this.channel.close();
        }
        if (this.connection) {
            await this.connection.close();
        }
        if (this.enableLog)
            console.log("[RabbitMQ] Disconnected");
    }
    async publish(topic, message) {
        if (!this.channel) {
            if (this.enableLog)
                console.error("RabbitMQ channel not initialized");
            return;
        }
        await this.channel.assertQueue(topic, { durable: false });
        this.channel.sendToQueue(topic, Buffer.from(JSON.stringify(message)));
    }
    async subscribe(topic, handler) {
        if (!this.channel) {
            if (this.enableLog)
                console.error("RabbitMQ channel not initialized");
            return;
        }
        await this.channel.assertQueue(topic, { durable: false });
        this.topicHandlers.set(topic, handler);
        await this.channel.consume(topic, async (rawMsg) => {
            if (!rawMsg)
                return;
            try {
                const decoded = JSON.parse(rawMsg.content.toString());
                await handler(decoded, rawMsg);
                this.channel.ack(rawMsg);
            }
            catch (err) {
                if (this.enableLog)
                    console.error("[RabbitMQ] Error in subscription handler:", err);
            }
        });
    }
    async reply(args) {
        const responseQueue = args.message?.metadata?.replyTo;
        if (!responseQueue) {
            throw new Error("ReplyTo queue not found in message metadata");
        }
        this.channel.sendToQueue(responseQueue, Buffer.from(JSON.stringify(args.message)), { correlationId: args.message.id });
    }
    async makeRequest(topic, message, timeout = 5000) {
        const correlationId = message.id;
        message = { ...message, metadata: { ...message.metadata, replyTo: this.responseQueue } };
        return new Promise(async (resolve, reject) => {
            const timer = setTimeout(() => {
                this.pendingRequests.delete(correlationId);
                reject(new Error(`RabbitMQ request timed out for ${topic}`));
            }, timeout);
            this.pendingRequests.set(correlationId, { resolve, reject, timer });
            await this.channel.assertQueue(topic, { durable: false });
            this.channel.sendToQueue(topic, Buffer.from(JSON.stringify(message)), {
                correlationId,
                replyTo: this.responseQueue,
            });
        });
    }
    setEnableLog(enable) {
        this.enableLog = enable;
    }
}
exports.RabbitMQProvider = RabbitMQProvider;
