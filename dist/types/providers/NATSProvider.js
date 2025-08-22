"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.NATSProvider = void 0;
const nats_1 = require("nats");
class NATSProvider {
    constructor(url) {
        this.url = url;
        this.codec = (0, nats_1.JSONCodec)();
        this.enableLog = true;
    }
    async connect() {
        this.connection = await (0, nats_1.connect)({ servers: this.url });
        if (this.enableLog)
            console.log(`[NATS] Connected to ${this.url}`);
    }
    async disconnect() {
        if (this.connection) {
            await this.connection.drain();
            if (this.enableLog)
                console.log("[NATS] Disconnected");
        }
    }
    async publish(subject, message) {
        if (!this.connection) {
            if (this.enableLog)
                console.error("NATS connection not established");
            return;
        }
        this.connection.publish(subject, this.codec.encode(message));
    }
    async subscribe(subject, handler) {
        if (!this.connection) {
            if (this.enableLog)
                console.error("NATS connection not established");
            return;
        }
        const sub = this.connection.subscribe(subject);
        for await (const m of sub) {
            try {
                const decoded = this.codec.decode(m.data);
                await handler(decoded, m);
            }
            catch (err) {
                if (this.enableLog)
                    console.error(`[NATS] Error handling message on ${subject}:`, err);
                return;
            }
        }
    }
    async reply(args) {
        if (!args.m.reply) {
            if (this.enableLog)
                console.error("No reply field found");
            return;
        }
        await this.publish(args.m.reply, args.message);
    }
    async makeRequest(subject, message) {
        const encoded = this.codec.encode(message);
        const resp = await this.connection.request(subject, encoded);
        if (!resp.data)
            throw new Error("Empty message");
        const decoded = this.codec.decode(resp.data);
        return decoded;
    }
    setEnableLog(enable) {
        this.enableLog = enable;
    }
}
exports.NATSProvider = NATSProvider;
