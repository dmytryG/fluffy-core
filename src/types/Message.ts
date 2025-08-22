export interface Message {
    req: any
    id: string
    isResponse: boolean
    resp: any
    isError: boolean | null | undefined
    safeMetadata: any // Will be removed before sending
}