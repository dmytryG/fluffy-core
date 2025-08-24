"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RedisStreamsProvider = void 0;
const redis_1 = require("redis");
const uuid_1 = require("uuid");
class RedisStreamsProvider {
    constructor(host, port, groupId, consumerId) {
        this.host = host;
        this.port = port;
        this.enableLog = true;
        this.pendingRequests = new Map();
        this.clientId = `fc-${(0, uuid_1.v4)()}`;
        this.responseStream = `response_${this.clientId}`;
        this.groupId = groupId ?? `grp-${this.clientId}`;
        this.consumerId = consumerId ?? this.clientId;
    }
    async ready() { }
    async connect() {
        this.client = (0, redis_1.createClient)({
            url: `redis://${this.host}:${this.port}`,
        });
        this.client.on("error", (err) => {
            console.error("[Redis] Client error:", err);
        });
        await this.client.connect();
        // Создадим consumer group для response stream
        try {
            await this.client.xGroupCreate(this.responseStream, this.groupId, "$", { MKSTREAM: true });
        }
        catch (err) {
            if (!err.message.includes("BUSYGROUP")) {
                throw err;
            }
        }
        // Запустим обработчик ответов
        this.consumeLoop(this.responseStream, async (decoded) => {
            if (this.enableLog)
                console.log(`[Redis] Received response:`, decoded);
            const corrId = decoded?.id;
            if (corrId && this.pendingRequests.has(corrId)) {
                const handler = this.pendingRequests.get(corrId);
                handler.resolve(decoded);
                clearTimeout(handler.timer);
                this.pendingRequests.delete(corrId);
            }
        });
        if (this.enableLog)
            console.log(`[Redis] Connected: ${this.host}:${this.port}`);
    }
    async disconnect() {
        if (this.client) {
            await this.client.disconnect();
        }
        if (this.enableLog)
            console.log("[Redis] Disconnected");
    }
    async publish(topic, message) {
        await this.client.xAdd(topic, "*", { data: JSON.stringify(message) });
    }
    async subscribe(topic, handler) {
        // создаём группу для стрима
        try {
            await this.client.xGroupCreate(topic, this.groupId, "$", { MKSTREAM: true });
        }
        catch (err) {
            if (!err.message.includes("BUSYGROUP"))
                throw err;
        }
        this.consumeLoop(topic, async (decoded, raw) => {
            try {
                await handler(decoded, raw);
            }
            catch (err) {
                if (this.enableLog)
                    console.error("[Redis] Error in subscription handler:", err);
            }
        });
    }
    async consumeLoop(stream, onMessage) {
        (async () => {
            while (true) {
                const res = await this.client.xReadGroup(this.groupId, this.consumerId, [{ key: stream, id: ">" }], { COUNT: 10, BLOCK: 5000 });
                if (!res)
                    continue;
                for (const streamRes of res) {
                    for (const msg of streamRes.messages) {
                        const raw = msg;
                        const decoded = JSON.parse(msg.message.data);
                        await onMessage(decoded, raw);
                        await this.client.xAck(stream, this.groupId, msg.id);
                    }
                }
            }
        })();
    }
    async reply(args) {
        const responseStream = args.message?.metadata?.replyTo;
        if (!responseStream)
            throw new Error("ReplyTo stream not found in message metadata");
        if (this.enableLog) {
            console.log(`[Redis] Replying to ${args.topic} with message:`, args.message);
        }
        await this.client.xAdd(responseStream, "*", { data: JSON.stringify(args.message) });
    }
    async makeRequest(topic, message, timeout = 5000) {
        const correlationId = message.id;
        message = {
            ...message,
            metadata: { ...message.metadata, replyTo: this.responseStream },
        };
        return new Promise(async (resolve, reject) => {
            const timer = setTimeout(() => {
                this.pendingRequests.delete(correlationId);
                reject(new Error(`Redis request timed out for ${topic}`));
            }, timeout);
            this.pendingRequests.set(correlationId, { resolve, reject, timer });
            if (this.enableLog) {
                console.log(`[Redis] Sending request to ${topic} with content`, message);
            }
            await this.client.xAdd(topic, "*", { data: JSON.stringify(message) });
        });
    }
    setEnableLog(enable) {
        this.enableLog = enable;
    }
}
exports.RedisStreamsProvider = RedisStreamsProvider;
