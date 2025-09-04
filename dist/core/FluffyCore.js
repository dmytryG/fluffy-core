"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const uuid_1 = require("uuid");
const APIError_1 = __importDefault(require("../types/APIError"));
class FluffyCore {
    constructor(provider) {
        this.pipelines = [];
        this.enableLog = true;
        this.provider = provider;
    }
    registerRoute({ ...args }) {
        this.pipelines.push({
            topic: args.topic,
            middlewares: args.middlewares ?? [],
            controller: args.controller,
            postware: args.postware ?? []
        });
    }
    setErrorProcessor(p) {
        this.errorProcessor = p;
    }
    async start() {
        this.provider.setEnableLog(this.enableLog);
        await this.provider.connect();
        for (const pipeline of this.pipelines) {
            this.provider.subscribe(pipeline.topic, async (msg, m) => {
                let result;
                try {
                    for (const middleware of pipeline.middlewares) {
                        await middleware(msg);
                    }
                    await pipeline.controller(msg);
                    for (const postware of pipeline.postware) {
                        await postware(msg);
                    }
                    result = {
                        ...msg,
                        isResponse: true,
                        isError: msg.isError === undefined ? false : msg.isError,
                    };
                }
                catch (e) {
                    if (this.enableLog)
                        console.error(e);
                    if (this.errorProcessor) {
                        await this.errorProcessor({ msg, e });
                        result = {
                            ...msg,
                            isResponse: true,
                        };
                    }
                    else {
                        if (this.enableLog)
                            console.warn('Consider using custom error controller, providing full error to a client can be dangerous to data privacy');
                        result = {
                            ...msg,
                            isResponse: true,
                            resp: JSON.stringify(e),
                            isError: true
                        };
                    }
                }
                result = {
                    ...result, safeMetadata: undefined
                };
                if (this.enableLog)
                    console.log(`${new Date().toISOString()} sending reply`, result);
                if (!result.noReply)
                    await this.provider.reply({ m, message: result, topic: pipeline.topic });
            });
        }
        await this.provider.ready();
    }
    async stop() {
        await this.provider.disconnect();
    }
    async fireAndForget({ data, topic, metadata }) {
        const outcoming = {
            req: data,
            id: (0, uuid_1.v4)(),
            isResponse: false,
            resp: undefined,
            isError: undefined,
            safeMetadata: undefined,
            noReply: true,
            metadata,
        };
        if (this.enableLog)
            console.log(`${new Date().toISOString()} prepared message to send`, outcoming, 'by topic', topic);
        await this.provider.publish(topic, outcoming);
    }
    async makeRequest({ data, topic, metadata }) {
        const outcoming = {
            req: data,
            id: (0, uuid_1.v4)(),
            isResponse: false,
            resp: undefined,
            isError: undefined,
            safeMetadata: undefined,
            metadata,
        };
        if (this.enableLog)
            console.log(`${new Date().toISOString()} prepared message to send`, outcoming, 'by topic', topic);
        const resp = await this.provider.makeRequest(topic, outcoming);
        if (this.enableLog)
            console.log(`${new Date().toISOString()} got response`, resp);
        if (resp.isError) {
            if (this.enableLog)
                console.error(`${new Date().toISOString()} response with error`, resp.resp);
        }
        return resp;
    }
    async makeRequestSimplify({ data, topic, metadata }) {
        const res = await this.makeRequest({ data, topic, metadata });
        if (res.isError)
            throw new APIError_1.default({ error: res.resp });
        return res.resp;
    }
    async broadcast({ data, topic, metadata }) {
        const outcoming = {
            req: data,
            id: (0, uuid_1.v4)(),
            isResponse: false,
            resp: undefined,
            isError: undefined,
            safeMetadata: undefined,
            metadata,
        };
        if (this.enableLog)
            console.log(`${new Date().toISOString()} prepared message to broadcast`, outcoming, 'by topic', topic);
        await this.provider.publish(topic, outcoming);
        return;
    }
}
exports.default = FluffyCore;
