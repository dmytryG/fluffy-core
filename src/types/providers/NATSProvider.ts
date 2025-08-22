import {connect, NatsConnection, JSONCodec, Subscription, Msg} from "nats";
import {IProvider} from "../../types/providers/IProvider";
import {Message} from "../../types/Message";

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

    async publish(subject: string, message: Message): Promise<void> {
        if (!this.connection) {
            if (this.enableLog) console.error("NATS connection not established");
            return
        }
        this.connection.publish(subject, this.codec.encode(message));
    }

    async subscribe(
        subject: string,
        handler: (msg: Message, m: any) => Promise<void> | void
    ): Promise<void> {
        if (!this.connection) {
            if (this.enableLog) console.error("NATS connection not established");
            return
        }

        const sub: Subscription = this.connection.subscribe(subject);
        for await (const m of sub) {
            try {
                const decoded = this.codec.decode(m.data) as Message;
                await handler(decoded, m);
            } catch (err) {
                if (this.enableLog) console.error(`[NATS] Error handling message on ${subject}:`, err);
                return
            }
        }
    }

    async reply(args: {message: Message, m: Msg}): Promise<void> {
        if (!args.m.reply) {
            if (this.enableLog) console.error("No reply field found");
            return
        }
        await this.publish(args.m.reply, args.message)
    }

    async makeRequest(subject: string, message: Message): Promise<Message> {
        const encoded = this.codec.encode(message)
        const resp = await this.connection.request(subject, encoded);
        if (!resp.data)
            throw new Error("Empty message")
        const decoded = this.codec.decode(resp.data) as Message;
        return decoded
    }

    setEnableLog(enable: boolean): void {
        this.enableLog = enable
    }
}
