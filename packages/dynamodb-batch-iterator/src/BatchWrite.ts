import { BatchOperation } from './BatchOperation';
import { itemIdentifier } from './itemIdentifier';
import { WriteRequest } from './types';
import { BatchWriteItemInput } from 'aws-sdk/clients/dynamodb';

export const MAX_WRITE_BATCH_SIZE = 25;

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
    protected readonly batchSize = MAX_WRITE_BATCH_SIZE;

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
                    for (let i = inFlight.length - 1; i >= 0; i--) {
                        const [tableName, attributes] = inFlight[i];
                        if (
                            tableName === table &&
                            itemIdentifier(tableName, attributes) === identifier
                        ) {
                            inFlight.splice(i, 1);
                        }
                    }
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
