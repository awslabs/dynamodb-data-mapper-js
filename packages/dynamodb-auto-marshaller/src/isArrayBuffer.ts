/**
 * Determines if the provided argument is an ArrayBuffer object. Compatible with
 * ArrayBuffers created in separate iframes and VMs.
 */
export function isArrayBuffer(arg: any): arg is ArrayBuffer {
    return (typeof ArrayBuffer === 'function' && arg instanceof ArrayBuffer) ||
        Object.prototype.toString.call(arg) === '[object ArrayBuffer]';
}
