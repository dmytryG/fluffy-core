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
        const postwares = args.postware ?
            args.postware.sort((a, b) => (a.priority ?? 0) - (b.priority ?? 0)).map((c) => c.controller)
            : [];
        this.pipelines.push({
            topic: args.topic,
            middlewares: args.middlewares ?? [],
            controller: args.controller,
            postware: postwares ?? []
        });
    }
    setErrorProcessor(p) {
        this.errorProcessor = p;
    }
    async start() {
        this.provider.setEnableLog(this.enableLog);
        await this.provider.connect();
        for (const pipeline of this.pipelines) {
            await this.provider.subscribe(pipeline.topic, async (msg, m) => {
                let result;
                try {
                    for (const middleware of pipeline.middlewares) {
                        msg = await middleware(msg);
                    }
                    msg = await pipeline.controller(msg);
                    for (const postware of pipeline.postware) {
                        msg = await postware(msg);
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
                        result = await this.errorProcessor({ msg, e });
                    }
                    else {
                        if (this.enableLog)
                            console.warn('Consider using custom error controller, providing full error to a client can be dangerous to data privacy');
                        result = {
                            ...msg,
                            isResponse: true,
                            resp: e,
                            isError: true
                        };
                    }
                }
                result = {
                    ...result, safeMetadata: undefined
                };
                if (this.enableLog)
                    console.log(`${new Date().toISOString()} sending reply`, result);
                await this.provider.reply(result, m);
            });
        }
    }
    async makeRequest({ data, topic }) {
        const outcoming = {
            req: data,
            id: (0, uuid_1.v4)(),
            isResponse: false,
            resp: undefined,
            isError: undefined,
            safeMetadata: undefined
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
    async makeRequestSimplify({ data, topic }) {
        const res = await this.makeRequest({ data, topic });
        if (res.isError)
            throw new APIError_1.default({ error: res.resp });
        return res.resp;
    }
}
exports.default = FluffyCore;
