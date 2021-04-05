import { BatchGetOptions, PerTableOptions } from './BatchGetOptions';
import { BatchOperation } from './BatchOperation';
import { SyncOrAsyncIterable, TableState } from './types';
import {AttributeValue, BatchGetItemInput, DynamoDB} from '@aws-sdk/client-dynamodb';

export const MAX_READ_BATCH_SIZE = 100;

/**
 * Retrieves items from DynamoDB in batches of 100 or fewer via one or more
 * BatchGetItem operations. The items may be from any number of tables.
 *
 * This method will automatically retry any get requests returned by DynamoDB as
 * unprocessed. Exponential backoff on unprocessed items is employed on a
 * per-table basis.
 */
export class BatchGet extends BatchOperation<{[key: string]: AttributeValue}> {
    protected readonly batchSize = MAX_READ_BATCH_SIZE;

    private readonly consistentRead?: boolean;
    private readonly options: PerTableOptions;

    /**
     * @param client    The AWS SDK client with which to communicate with
     *                  DynamoDB.
     * @param items     A synchronous or asynchronous iterable of tuples
     *                  describing the reads to execute. The first member of the
     *                  tuple should be the name of the table from which to
     *                  read, and the second should be the marshalled key.
     * @param options   Additional options to apply to the operations executed.
     */
    constructor(
        client: DynamoDB,
        items: SyncOrAsyncIterable<[string, {[key: string]: AttributeValue}]>,
        {
            ConsistentRead,
            PerTableOptions = {},
        }: BatchGetOptions = {}
    ) {
        super(client, items);
        this.consistentRead = ConsistentRead;
        this.options = PerTableOptions;
    }

    protected async doBatchRequest() {
        let operationInput: BatchGetItemInput = {RequestItems: {}};
        let batchSize = 0;

        while (this.toSend.length > 0) {
            const [tableName, item] = this.toSend.shift() as [string, {[key: string]: AttributeValue}];
            if(operationInput.RequestItems === undefined) {
                operationInput.RequestItems = {};
            }

            if (operationInput.RequestItems[tableName] === undefined) {
                const {
                    projection,
                    consistentRead,
                    attributeNames,
                } = this.state[tableName];

                operationInput.RequestItems[tableName] = {
                    Keys: [],
                    ConsistentRead: consistentRead,
                    ProjectionExpression: projection,
                    ExpressionAttributeNames: attributeNames,
                };
            }

            // @ts-ignore I can't see why operationInput.RequestItems[tableName].Keys can be undefined
            operationInput.RequestItems[tableName].Keys.push(item);

            if (++batchSize === this.batchSize) {
                break;
            }
        }

        const {
            Responses = {},
            UnprocessedKeys = {},
        } = await this.client.batchGetItem(operationInput);

        const unprocessedTables = new Set<string>();
        for (const table of Object.keys(UnprocessedKeys)) {
            unprocessedTables.add(table);
            if(UnprocessedKeys[table].Keys === undefined) {
                continue;
            }

            this.handleThrottled(table, UnprocessedKeys[table].Keys!);
        }

        this.movePendingToThrottled(unprocessedTables);

        for (const table of Object.keys(Responses)) {
            const tableData = this.state[table];
            tableData.backoffFactor = Math.max(0, tableData.backoffFactor - 1);
            for (const item of Responses[table]) {
                this.pending.push([table, item]);
            }
        }
    }

    protected getInitialTableState(tableName: string): TableState<{[key: string]: AttributeValue}> {
        let options = this.options[tableName];

        if(options === undefined) {
            options = {};
        }

        return {
            ...super.getInitialTableState(tableName),
            attributeNames: options.ExpressionAttributeNames,
            projection: options.ProjectionExpression,
            consistentRead: options.ConsistentRead || this.consistentRead
        };
    }
}
