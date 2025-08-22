export interface IProvider {
    connect(): Promise<void>;
    disconnect(): Promise<void>;
    setEnableLog(enable: boolean): void

    publish<T = any>(subject: string, message: T): Promise<void>;

    subscribe<T = any>(
        subject: string,
        handler: (msg: T, m: any) => Promise<void> | void
    ): Promise<void>;

    reply<T = any>(message: T, m: any): Promise<void>;

    makeRequest<T = any>(subject: string, message: T): Promise<T>;
}
