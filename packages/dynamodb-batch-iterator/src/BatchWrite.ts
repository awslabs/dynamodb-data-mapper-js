import { BatchOperation } from './BatchOperation';
import { fromUtf8 } from './fromUtf8';
import { WriteRequest } from './types';
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
 * member should be the table name, and the second should be {WriteRequest}
 * object that defines either a put request or a delete request.
 *
 * This method will automatically retry any write requests returned by DynamoDB
 * as unprocessed. Exponential backoff on unprocessed items is employed on a
 * per-table basis.
 */
export class BatchWrite extends BatchOperation<WriteRequest> {
    readonly batchSize = MAX_WRITE_BATCH_SIZE;

    protected async doBatchRequest() {
        const inFlight: Array<[string, WriteRequest]> = [];
        const operationInput: BatchWriteItemInput = {RequestItems: {}};

        let batchSize = 0;
        while (this.toSend.length > 0) {
            const [
                tableName,
                marshalled
            ] = this.toSend.shift() as [string, WriteRequest];

            inFlight.push([tableName, marshalled]);

            if (operationInput.RequestItems[tableName] === undefined) {
                operationInput.RequestItems[tableName] = [];
            }
            operationInput.RequestItems[tableName].push(marshalled);

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
            const unprocessed: Array<WriteRequest> = [];
            for (const item of UnprocessedItems[table]) {
                if (item.DeleteRequest || item.PutRequest) {
                    unprocessed.push(item as WriteRequest);

                    const identifier = itemIdentifier(table, item as WriteRequest);
                    inFlight.filter(
                        ([tableName, attributes]) => tableName !== table ||
                            itemIdentifier(tableName, attributes) !== identifier
                    );
                }
            }

            this.handleThrottled(table, unprocessed);
        }

        this.movePendingToThrottled(unprocessedTables);

        const processedTables = new Set<string>();
        for (const [tableName, marshalled] of inFlight) {
            processedTables.add(tableName);
            this.pending.push([tableName, marshalled]);
        }

        for (const tableName of processedTables) {
            this.state[tableName].backoffFactor =
                Math.max(0, this.state[tableName].backoffFactor - 1);
        }
    }
}

function itemIdentifier(tableName: string, request: WriteRequest): string {
    if (request.DeleteRequest) {
        return `${tableName}::delete::${serializeKeyTypeAttributes(request.DeleteRequest.Key)}`;
    } else if (request.PutRequest) {
        return `${tableName}::put::${serializeKeyTypeAttributes(request.PutRequest.Item)}`;
    }

    return tableName;
}

function serializeKeyTypeAttributes(attributes: AttributeMap): string {
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

function isArrayBuffer(arg: any): arg is ArrayBuffer {
    return (typeof ArrayBuffer === 'function' && arg instanceof ArrayBuffer) ||
        Object.prototype.toString.call(arg) === '[object ArrayBuffer]';
}
