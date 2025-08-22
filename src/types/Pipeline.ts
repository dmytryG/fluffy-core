import {ControllerNode} from "~/types/ControllerNode";

export interface Pipeline {
    topic: string
    middlewares: Array<ControllerNode>
    controller: ControllerNode
    postware: Array<ControllerNode>
}