import {IProvider} from "../types/providers/IProvider";
import {Pipeline} from "../types/Pipeline";
import {ControllerNode} from "../types/ControllerNode";
import {ErrorControllerNode} from "../types/ErrorControllerNode";
import {Message} from "../types/Message";
import { v4 as uuidv4 } from 'uuid';
import APIError from "../types/APIError";

export default class FluffyCore {
    private provider: IProvider;
    private pipelines: Pipeline[] = [];
    private errorProcessor: ErrorControllerNode;
    public enableLog: boolean = true;

    constructor(provider: IProvider) {
        this.provider = provider
    }

    registerRoute({ ...args }:
        { topic: string
        middlewares: Array<ControllerNode> | undefined | null
        controller: ControllerNode
        postware: Array<{ controller: ControllerNode, priority: number | undefined | null }> | undefined | null }): void {
        const postwares = args.postware ?
            args.postware.sort((a, b) => (a.priority ?? 0) - (b.priority ?? 0)).map((c) => c.controller)
            : []
        this.pipelines.push({
            topic: args.topic,
            middlewares: args.middlewares ?? [],
            controller: args.controller,
            postware: postwares ?? []
        })
    }

    setErrorProcessor(p: ErrorControllerNode): void {
        this.errorProcessor = p;
    }

    async start() {
        this.provider.setEnableLog(this.enableLog)
        await this.provider.connect();

        for (const pipeline of this.pipelines) {
            this.provider.subscribe(pipeline.topic, async (msg: Message, m: any) => {
                let result: Message;
                try {
                    for (const middleware of pipeline.middlewares) {
                        await middleware(msg)
                    }
                    await pipeline.controller(msg)
                    for (const postware of pipeline.postware) {
                        await postware(msg)
                    }
                    result = {
                        ...msg,
                        isResponse: true,
                        isError: msg.isError === undefined ? false : msg.isError,
                    }
                } catch (e) {
                    if (this.enableLog) console.error(e)
                    if (this.errorProcessor) {
                        await this.errorProcessor({ msg, e })
                        result = {
                            ...msg,
                            isResponse: true,
                        }
                    } else {
                        if (this.enableLog) console.warn('Consider using custom error controller, providing full error to a client can be dangerous to data privacy')
                        result = {
                            ...msg,
                            isResponse: true,
                            resp: e,
                            isError: true
                        }
                    }
                }
                result = {
                    ...result, safeMetadata: undefined
                }
                if (this.enableLog) console.log(`${new Date().toISOString()} sending reply`, result)
                await this.provider.reply(result, m)
            });
        }
    }

    async makeRequest({data, topic}: {data: any, topic: string}): Promise<Message> {
        const outcoming: Message = {
            req: data,
            id: uuidv4(),
            isResponse: false,
            resp: undefined,
            isError: undefined,
            safeMetadata: undefined
        }
        if (this.enableLog) console.log(`${new Date().toISOString()} prepared message to send`, outcoming, 'by topic', topic)
        const resp = await this.provider.makeRequest(topic, outcoming)
        if (this.enableLog) console.log(`${new Date().toISOString()} got response`, resp)
        if (resp.isError) {
            if (this.enableLog) console.error(`${new Date().toISOString()} response with error`, resp.resp)
        }
        return resp
    }

    async makeRequestSimplify({data, topic}: {data: any, topic: string}): Promise<any> {
        const res = await this.makeRequest({ data, topic })
        if (res.isError) throw new APIError({ error: res.resp })
        return res.resp
    }

}