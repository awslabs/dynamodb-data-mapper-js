import { BatchOperation } from './BatchOperation';
import { WritePair, WriteType } from './BatchTypes';
import {
    AttributeMap,
    BatchWriteItemInput,
    BinaryAttributeValue,
} from 'aws-sdk/clients/dynamodb';

const MAX_WRITE_BATCH_SIZE = 25;

/**
 * Puts or deletes items from DynamoDB in batches of 25 or fewer via one or more
 * BatchWriteItem operations. The items may belong to any number of tables.
 *
 * The iterable of writes to perform may be synchronous or asynchronous and is
 * expected to yield tuples describing the writes to be performed. The first
 * member should be the table name, and the second should be a tuple of the form
 * `['put'|'delete', AttributeMap]`. 
 *
 * This method will automatically retry any write requests returned by DynamoDB
 * as unprocessed. Exponential backoff on unprocessed items is employed on a
 * per-table basis.
 */
export class BatchWrite extends BatchOperation<WritePair> {
    readonly batchSize = MAX_WRITE_BATCH_SIZE;

    protected async doBatchRequest() {
        const inFlight: Array<[WriteType, string, AttributeMap]> = [];
        const operationInput: BatchWriteItemInput = {RequestItems: {}};

        let batchSize = 0;
        while (this.toSend.length > 0) {
            const [
                tableName,
                [type, marshalled]
            ] = this.toSend.shift() as [string, WritePair];

            inFlight.push([type, tableName, marshalled]);

            if (operationInput.RequestItems[tableName] === undefined) {
                operationInput.RequestItems[tableName] = [];
            }
            operationInput.RequestItems[tableName].push(
                type === 'delete'
                    ? {DeleteRequest: {Key: marshalled}}
                    : {PutRequest: {Item: marshalled}}
            );

            if (++batchSize === this.batchSize) {
                break;
            }
        }

        const {
            UnprocessedItems = {}
        } = await this.client.batchWriteItem(operationInput).promise();
        const unprocessedTables = new Set<string>();

        for (const table of Object.keys(UnprocessedItems)) {
            unprocessedTables.add(table);
            const unprocessed: Array<WritePair> = [];
            for (const item of UnprocessedItems[table]) {
                if (item.DeleteRequest) {
                    const {Key} = item.DeleteRequest;
                    unprocessed.push(['delete', Key]);

                    const identifier = itemIdentifier(table, Key);
                    inFlight.filter(
                        ([type, tableName, attributes]) => type !== 'delete' ||
                            tableName !== table ||
                            itemIdentifier(tableName, attributes) !== identifier
                    );
                } else if (item.PutRequest) {
                    const {Item} = item.PutRequest;
                    unprocessed.push(['put', Item]);

                    const identifier = itemIdentifier(table, Item);
                    inFlight.filter(
                        ([type, tableName, attributes]) => type !== 'put' ||
                            tableName !== table ||
                            itemIdentifier(tableName, attributes) !== identifier
                    );
                }
            }

            this.handleThrottled(table, unprocessed);
        }

        this.movePendingToThrottled(unprocessedTables);

        const processedTables = new Set<string>();
        for (const [type, tableName, marshalled] of inFlight) {
            processedTables.add(tableName);
            if (type === 'delete') {
                continue;
            }

            this.pending.push([tableName, marshalled]);
        }

        for (const tableName of processedTables) {
            const tableData = this.state[tableName];
            tableData.backoffFactor = Math.max(0, tableData.backoffFactor - 1);
        }
    }
}

function itemIdentifier(tableName: string, attributes: AttributeMap): string {
    const keyTypeProperties: Array<string> = [];
    for (const property of Object.keys(attributes).sort()) {
        const attribute = attributes[property];
        if (attribute.B) {
            keyTypeProperties.push(`${property}=${toByteArray(attribute.B)}`);
        } else if (attribute.N) {
            keyTypeProperties.push(`${property}=${toByteArray(attribute.N)}`);
        } else if (attribute.S) {
            keyTypeProperties.push(`${property}=${toByteArray(attribute.S)}`);
        }
    }

    return `${tableName}::${keyTypeProperties.join('&')}`;
}

function toByteArray(value: BinaryAttributeValue): Uint8Array {
    if (ArrayBuffer.isView(value)) {
        return new Uint8Array(
            value.buffer,
            value.byteOffset,
            value.byteLength
        );
    }

    if (typeof value === 'string') {
        return fromUtf8(value);
    }

    if (isArrayBuffer(value)) {
        return new Uint8Array(value);
    }

    throw new Error('Unrecognized binary type');
}

function fromUtf8(input: string): Uint8Array {
    const bytes: Array<number> = [];
    for (let i = 0, len = input.length; i < len; i++) {
        const value = input.charCodeAt(i);
        if (value < 0x80) {
            bytes.push(value);
        } else if (value < 0x800) {
            bytes.push(
                (value >> 6) | 0b11000000,
                (value & 0b111111) | 0b10000000
            );
        } else if (
            i + 1 < input.length &&
            ((value & 0xfc00) === 0xd800) &&
            ((input.charCodeAt(i + 1) & 0xfc00) === 0xdc00)
        ) {
            const surrogatePair = 0x10000 +
                ((value & 0b1111111111) << 10) +
                (input.charCodeAt(++i) & 0b1111111111);
            bytes.push(
                (surrogatePair >> 18) | 0b11110000,
                ((surrogatePair >> 12) & 0b111111) | 0b10000000,
                ((surrogatePair >> 6) & 0b111111) | 0b10000000,
                (surrogatePair & 0b111111) | 0b10000000
            );
        } else {
            bytes.push(
                (value >> 12) | 0b11100000,
                ((value >> 6) & 0b111111) | 0b10000000,
                (value & 0b111111) | 0b10000000,
            );
        }
    }

    return Uint8Array.from(bytes);
}

function isArrayBuffer(arg: any): arg is ArrayBuffer {
    return (typeof ArrayBuffer === 'function' && arg instanceof ArrayBuffer) ||
        Object.prototype.toString.call(arg) === '[object ArrayBuffer]';
}
