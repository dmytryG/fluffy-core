// export default class Logger {
//     private static _isEnabled = true;
//
//     static get isEnabled(): boolean {
//         return this._isEnabled;
//     }
//
//     static set isEnabled(value: boolean) {
//         this._isEnabled = value;
//     }
//
//     static async log(...args: any[]): Promise<void> {
//         if (!Logger.isEnabled) return
//         console.log((new Date()).toISOString(), ...args)
//     }
//     static async error(...args: any[]): Promise<void> {
//         if (!Logger.isEnabled) return
//         console.error((new Date()).toISOString(), ...args)
//     }
// }