import {Message} from "../../types/Message";

export interface IProvider {
    connect(): Promise<void>;
    disconnect(): Promise<void>;
    setEnableLog(enable: boolean): void

    publish(subject: string, message: Message): Promise<void>;

    subscribe(
        subject: string,
        handler: (msg: Message, m: any) => Promise<void> | void
    ): Promise<void>;

    reply({
        message,
        m,
        topic,
                   } : {
                       message: Message,
                       m?: any, // for nats
                       topic?: string
    }
    ): Promise<void>;

    makeRequest(subject: string, message: Message): Promise<Message>;
}
