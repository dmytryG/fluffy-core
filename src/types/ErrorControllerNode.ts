import {Message} from "../types/Message";

export type ErrorControllerNode = ({msg, e}: { msg: Message, e: any }) => Promise<Message>;