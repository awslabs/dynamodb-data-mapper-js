import { WriteRequest } from './types';
import {AttributeValue} from "@aws-sdk/client-dynamodb";
const bytes = require('utf8-bytes');

/**
 * @internal
 */
export function itemIdentifier(
    tableName: string,
    {DeleteRequest, PutRequest}: WriteRequest
): string {
    if (DeleteRequest && DeleteRequest.Key) {
        return `${tableName}::delete::${
            serializeKeyTypeAttributes(DeleteRequest.Key)
        }`;
    } else if (PutRequest && PutRequest.Item) {
        return `${tableName}::put::${
            serializeKeyTypeAttributes(PutRequest.Item)
        }`;
    }

    throw new Error(`Invalid write request provided`);
}

function serializeKeyTypeAttributes(attributes: {[key: string]: AttributeValue}): string {
    const keyTypeProperties: Array<string> = [];
    for (const property of Object.keys(attributes).sort()) {
        const attribute = attributes[property];
        if (attribute.B) {
            keyTypeProperties.push(`${property}=${toByteArray(attribute.B)}`);
        } else if (attribute.N) {
            keyTypeProperties.push(`${property}=${attribute.N}`);
        } else if (attribute.S) {
            keyTypeProperties.push(`${property}=${attribute.S}`);
        }
    }

    return keyTypeProperties.join('&');
}

function toByteArray(value: Uint8Array): Uint8Array {
    if (ArrayBuffer.isView(value)) {
        return new Uint8Array(
            value.buffer,
            value.byteOffset,
            value.byteLength
        );
    }

    if (typeof value === 'string') {
        return Uint8Array.from(bytes(value));
    }

    if (isArrayBuffer(value)) {
        return new Uint8Array(value);
    }

    throw new Error('Unrecognized binary type');
}

function isArrayBuffer(arg: any): arg is ArrayBuffer {
    return (typeof ArrayBuffer === 'function' && arg instanceof ArrayBuffer) ||
        Object.prototype.toString.call(arg) === '[object ArrayBuffer]';
}
