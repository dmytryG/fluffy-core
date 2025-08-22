"use strict";
// import WebSocket from 'ws';
// import { Message } from '../types/Message'; // Assuming Message type is
// import { v4 as uuidv4 } from 'uuid';
// import {RequestConfig} from "../types/RequestConfig";
// import {RequestCallback} from "../types/RequestCallback";
// import Logger from "./Logger";
// import APIError from "../types/APIError";
// import {Endpoint} from "../types/Endpoint";
//
// export class Client {
//     private ws: WebSocket | null = null;
//     private connected: boolean = false
//     private pendingRequests: Map<string, RequestCallback>;
//     private listeners: Map<string, Endpoint>;
//     private _onCloseListener: () => void | undefined;
//
//     constructor(private url: string) {
//         this.pendingRequests = new Map()
//         this.listeners = new Map()
//     }
//
//     set onCloseListener(value: () => (void | undefined)) {
//         this._onCloseListener = value;
//     }
//
//     protected processIncoming(client: Client): any {
//         return (msg: WebSocket.MessageEvent) => {
//             try {
//                 // @ts-ignore
//                 const parsed: Message = JSON.parse(msg.data)
//                 Logger.log("Client got message", parsed, "with id", parsed.id)
//                 Logger.log(`There is ${client.pendingRequests.size} pending requests`)
//                 const pending = client.pendingRequests.get(parsed.id)
//                 const listener = client.listeners.get(parsed.topic)
//                 if (pending) {
//                     pending.callback(parsed)
//                 } else if (listener) {
//                     listener.function(parsed, undefined)
//                 } else {
//                     return
//                 }
//             } catch (e) {
//                 Logger.error("Error in incoming message loop", e)
//             }
//         }
//     }
//
//     public async connect(): Promise<void> {
//         Logger.log("Connecting client ws")
//         this.ws = new WebSocket(this.url);
//
//         await new Promise((resolve, reject) => {
//             this.ws!.on('open', resolve);
//             this.ws!.on('error', reject);
//         });
//
//         this.ws!.onmessage = this.processIncoming(this)
//
//         this.ws!.on('close', this.onClose);
//
//         this.connected = true;
//         await Logger.log("Socket successfully connected")
//     }
//
//     public async request(config: Partial<RequestConfig>): Promise<Message> {
//         Logger.log("Requesting", config)
//         return new Promise((resolve, reject) => {
//             if (!this.connected) {
//                 throw new APIError('WebSocket client is not connected', 404);
//             }
//             if (!config.topic) {
//                 throw new APIError('Topic have to be provided', 400);
//             }
//             const messageId = uuidv4()
//             const message = {
//                 data: config.data,
//                 context: config.context,
//                 id: messageId,
//                 topic: config.topic,
//                 isResponse: false,
//                 client: undefined,
//                 isError: false
//             } as Message
//
//             const JSONToSend = JSON.stringify(message)
//
//             const timeout = setTimeout(() => {
//                 this.pendingRequests.delete(messageId); // Clear pending request
//                 reject(new APIError('Request timed out', 408));
//             }, config.timeout ?? 10000);
//
//             const onResolved = (result: Message) => {
//                 this.pendingRequests.delete(messageId)
//                 Logger.log("Request resolved", result)
//                 clearTimeout(timeout)
//                 if (result.isError) {
//                     const error = new APIError(result.data.message, result.data.code, result.data.reason)
//                     reject(error)
//                 }
//                 resolve(result)
//             }
//
//             this.pendingRequests.set(messageId, { request: message, callback: onResolved })
//
//             this.ws!.send(JSONToSend)
//
//             Logger.log("Sent request", JSONToSend)
//         })
//     }
//
//     public subscribeListener(listener: Endpoint): void {
//         this.listeners.set(listener.topic, listener)
//     }
//
//     public unsubscribeListener(topic: string): void {
//         this.listeners.delete(topic)
//     }
//
//     public getListener(topic: string): Endpoint | undefined {
//         const listener = this.listeners.get(topic)
//         return listener
//     }
//
//     public async send(config: RequestConfig): Promise<void> {
//         if (!this.connected) {
//             throw new APIError('WebSocket client is not connected', 404);
//         }
//         if (!config.topic) {
//             throw new APIError('Topic have to be provided', 400);
//         }
//         const messageId = uuidv4()
//         const message = {
//             data: config.data,
//             context: config.context,
//             id: messageId,
//             topic: config.topic,
//             isResponse: false,
//             client: undefined,
//             isError: false
//         } as Message
//
//         const JSONToSend = JSON.stringify(message)
//         this.ws!.send(JSONToSend)
//     }
//
//     public onClose(): void {
//         Logger.log("Called logging out of WS")
//         this.connected = false;
//         this._onCloseListener()
//     }
//
//     public close(): void {
//         if (this.ws) {
//             this.ws.close();
//
//         }
//         this.connected = false;
//     }
// }
