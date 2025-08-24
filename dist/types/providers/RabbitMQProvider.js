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
        // responseQueue -> (correlationId -> handler)
        this.pendingRequests = new Map();
        this.clientId = `fc-${(0, uuid_1.v4)()}`;
    }
    async ready() { }
    async connect() {
        this.connection = await amqplib_1.default.connect(this.url);
        this.channel = await this.connection.createChannel();
        if (this.enableLog)
            console.log(`[RabbitMQ] Connected to ${this.url}`);
    }
    async disconnect() {
        if (this.channel)
            await this.channel.close();
        if (this.connection)
            await this.connection.close();
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
        await this.channel.consume(topic, async (msg) => {
            if (!msg)
                return;
            try {
                const decoded = JSON.parse(msg.content.toString());
                await handler(decoded, msg);
                this.channel.ack(msg);
            }
            catch (err) {
                if (this.enableLog)
                    console.error(`[RabbitMQ] Error handling message on ${topic}:`, err);
                this.channel.nack(msg, false, false);
            }
        });
        if (this.enableLog)
            console.log(`[RabbitMQ] Subscribed to ${topic}`);
    }
    async reply(args) {
        const responseTopic = `response_${args.topic}`;
        await this.publish(responseTopic, args.message);
    }
    async makeRequest(topic, message, timeout = 5000) {
        const correlationId = message.id;
        const responseQueue = `response_${this.clientId}_${topic}`;
        if (!this.pendingRequests.has(responseQueue)) {
            this.pendingRequests.set(responseQueue, new Map());
            await this.channel.assertQueue(responseQueue, { durable: false });
            await this.channel.consume(responseQueue, (msg) => {
                if (!msg)
                    return;
                try {
                    const decoded = JSON.parse(msg.content.toString());
                    const correlationId = decoded.id;
                    const topicHandlers = this.pendingRequests.get(responseQueue);
                    if (!topicHandlers)
                        return;
                    const handler = topicHandlers.get(correlationId);
                    if (handler) {
                        handler.resolve(decoded);
                        clearTimeout(handler.timer);
                        topicHandlers.delete(correlationId);
                        if (topicHandlers.size === 0) {
                            this.pendingRequests.delete(responseQueue);
                            if (this.enableLog)
                                console.log(`[RabbitMQ] No more pending handlers for ${responseQueue}`);
                        }
                    }
                    this.channel.ack(msg);
                }
                catch (err) {
                    if (this.enableLog)
                        console.error(`[RabbitMQ] Error in response handler:`, err);
                    this.channel.nack(msg, false, false);
                }
            });
            if (this.enableLog)
                console.log(`[RabbitMQ] Subscribed to response queue ${responseQueue}`);
        }
        return new Promise(async (resolve, reject) => {
            const timer = setTimeout(() => {
                const topicHandlers = this.pendingRequests.get(responseQueue);
                if (topicHandlers)
                    topicHandlers.delete(correlationId);
                reject(new Error(`RabbitMQ request timed out for ${topic}`));
            }, timeout);
            this.pendingRequests.get(responseQueue).set(correlationId, { resolve, reject, timer });
            await this.publish(topic, message);
        });
    }
    setEnableLog(enable) {
        this.enableLog = enable;
    }
}
exports.RabbitMQProvider = RabbitMQProvider;
