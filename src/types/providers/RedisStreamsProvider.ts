import { createClient, RedisClientType } from "redis";
import { IProvider } from "../../types/providers/IProvider";
import { Message } from "../../types/Message";
import { v4 as uuidv4 } from "uuid";

type PendingHandler = {
    resolve: (value: any) => void;
    reject: (reason?: any) => void;
    timer: NodeJS.Timeout;
};

export class RedisStreamsProvider implements IProvider {
    private client!: RedisClientType;
    public enableLog: boolean = true;
    private responseStream: string;
    private clientId: string;
    private groupId: string;
    private consumerId: string;

    private pendingRequests: Map<string, PendingHandler> = new Map();

    constructor(
        private readonly host: string,
        private readonly port: number,
        groupId?: string,
        consumerId?: string
    ) {
        this.clientId = `fc-${uuidv4()}`;
        this.responseStream = `response_${this.clientId}`;
        this.groupId = groupId ?? `grp-${this.clientId}`;
        this.consumerId = consumerId ?? this.clientId;
    }

    async ready(): Promise<void> {}

    async connect(): Promise<void> {
        this.client = createClient({
            url: `redis://${this.host}:${this.port}`,
        });

        this.client.on("error", (err) => {
            console.error("[Redis] Client error:", err);
        });

        await this.client.connect();

        // Создадим consumer group для response stream
        try {
            await this.client.xGroupCreate(
                this.responseStream,
                this.groupId,
                "$",
                { MKSTREAM: true }
            );
        } catch (err: any) {
            if (!err.message.includes("BUSYGROUP")) {
                throw err;
            }
        }

        // Запустим обработчик ответов
        this.consumeLoop(this.responseStream, async (decoded) => {
            if (this.enableLog) console.log(`[Redis] Received response:`, decoded);
            const corrId = decoded?.id;
            if (corrId && this.pendingRequests.has(corrId)) {
                const handler = this.pendingRequests.get(corrId)!;
                handler.resolve(decoded);
                clearTimeout(handler.timer);
                this.pendingRequests.delete(corrId);
            }
        });

        if (this.enableLog) console.log(`[Redis] Connected: ${this.host}:${this.port}`);
    }

    async disconnect(): Promise<void> {
        if (this.client) {
            await this.client.disconnect();
        }
        if (this.enableLog) console.log("[Redis] Disconnected");
    }

    async publish(topic: string, message: Message): Promise<void> {
        await this.client.xAdd(topic, "*", { data: JSON.stringify(message) });
    }

    async subscribe(
        topic: string,
        handler: (msg: Message, raw: any) => Promise<void> | void
    ): Promise<void> {
        // создаём группу для стрима
        try {
            await this.client.xGroupCreate(topic, this.groupId, "$", { MKSTREAM: true });
        } catch (err: any) {
            if (!err.message.includes("BUSYGROUP")) throw err;
        }

        this.consumeLoop(topic, async (decoded, raw) => {
            try {
                await handler(decoded, raw);
            } catch (err) {
                if (this.enableLog) console.error("[Redis] Error in subscription handler:", err);
            }
        });
    }

    private async consumeLoop(
        stream: string,
        onMessage: (decoded: Message, raw: any) => Promise<void> | void
    ) {
        (async () => {
            while (true) {
                const res = await this.client.xReadGroup(
                    this.groupId,
                    this.consumerId,
                    [{ key: stream, id: ">" }],
                    { COUNT: 10, BLOCK: 5000 }
                );
                if (!res) continue;

                for (const streamRes of res) {
                    for (const msg of streamRes.messages) {
                        const raw = msg;
                        const decoded = JSON.parse(msg.message.data) as Message;
                        await onMessage(decoded, raw);
                        await this.client.xAck(stream, this.groupId, msg.id);
                    }
                }
            }
        })();
    }

    async reply(args: { topic: string; message: Message }): Promise<void> {
        const responseStream = args.message?.metadata?.replyTo;
        if (!responseStream) throw new Error("ReplyTo stream not found in message metadata");

        if (this.enableLog) {
            console.log(`[Redis] Replying to ${args.topic} with message:`, args.message);
        }

        await this.client.xAdd(responseStream, "*", { data: JSON.stringify(args.message) });
    }

    async makeRequest(topic: string, message: Message, timeout = 5000): Promise<Message> {
        const correlationId = message.id;
        message = {
            ...message,
            metadata: { ...message.metadata, replyTo: this.responseStream },
        };

        return new Promise<Message>(async (resolve, reject) => {
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

    setEnableLog(enable: boolean): void {
        this.enableLog = enable;
    }
}
