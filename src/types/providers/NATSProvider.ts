import {connect, NatsConnection, JSONCodec, Subscription, Msg} from "nats";
import {IProvider} from "../../types/providers/IProvider";

export class NATSProvider implements IProvider {
    private connection!: NatsConnection;
    private codec = JSONCodec();
    public enableLog: boolean = true;

    constructor(private readonly url: string) {}

    async connect(): Promise<void> {
        this.connection = await connect({ servers: this.url });
        if (this.enableLog) console.log(`[NATS] Connected to ${this.url}`);
    }

    async disconnect(): Promise<void> {
        if (this.connection) {
            await this.connection.drain();
            if (this.enableLog) console.log("[NATS] Disconnected");
        }
    }

    async publish<T = any>(subject: string, message: T): Promise<void> {
        if (!this.connection) {
            if (this.enableLog) console.error("NATS connection not established");
            return
        }
        this.connection.publish(subject, this.codec.encode(message));
    }

    async subscribe<T = any>(
        subject: string,
        handler: (msg: T, m: any) => Promise<void> | void
    ): Promise<void> {
        if (!this.connection) {
            if (this.enableLog) console.error("NATS connection not established");
            return
        }

        const sub: Subscription = this.connection.subscribe(subject);
        for await (const m of sub) {
            try {
                const decoded = this.codec.decode(m.data) as T;
                await handler(decoded, m);
            } catch (err) {
                if (this.enableLog) console.error(`[NATS] Error handling message on ${subject}:`, err);
                return
            }
        }
    }

    async reply<T = any>(message: T, m: Msg): Promise<void> {
        if (!m.reply) {
            if (this.enableLog) console.error("No reply field found");
            return
        }
        await this.publish(m.reply, m)
    }

    async makeRequest<T = any>(subject: string, message: T): Promise<T> {
        const encoded = this.codec.encode(message)
        const resp = await this.connection.request(subject, encoded);
        if (!resp.data)
            throw new Error("Empty message")
        const decoded = this.codec.decode(resp.data) as T;
        return decoded
    }

    setEnableLog(enable: boolean): void {
        this.enableLog = enable
    }
}
