import {Message} from "../types/Message";

export type ControllerNode = (msg: Message) => Promise<void>;