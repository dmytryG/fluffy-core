export interface Message {
    req: any
    id: string
    isResponse: boolean
    resp: any
    isError: boolean | undefined
    safeMetadata: any // Will be removed before sending
    metadata?: any | undefined
    noResponse?: boolean | undefined
}