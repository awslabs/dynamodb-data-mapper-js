import { DynamoDbPaginatorInterface } from './DynamoDbPaginatorInterface';
import { DynamoDbResultsPage } from './DynamoDbResultsPage';
import { mergeConsumedCapacities } from './mergeConsumedCapacities';
import {AttributeValue, ConsumedCapacity} from '@aws-sdk/client-dynamodb';

if (Symbol && !Symbol.asyncIterator) {
    (Symbol as any).asyncIterator = Symbol.for("__@@asyncIterator__");
}

export abstract class DynamoDbPaginator implements DynamoDbPaginatorInterface {
    private _consumedCapacity?: ConsumedCapacity;
    private _count = 0;
    private _lastKey?: {[key: string]: AttributeValue};
    private _scannedCount = 0;
    private lastResolved: Promise<IteratorResult<DynamoDbResultsPage>>
        = <any>Promise.resolve();

    protected constructor(private readonly limit?: number) {}

    /**
     * @inheritDoc
     */
    [Symbol.asyncIterator](): AsyncIterableIterator<DynamoDbResultsPage> {
        return this;
    }

    /**
     * @inheritDoc
     */
    get consumedCapacity(): ConsumedCapacity|undefined {
        return this._consumedCapacity;
    }

    /**
     * @inheritDoc
     */
    get count(): number {
        return this._count;
    }

    /**
     * Get the LastEvaluatedKey of the last result page yielded by this
     * paginator or undefined if the scan has already been exhausted.
     */
    get lastEvaluatedKey(): {[key: string]: AttributeValue}|undefined {
        return this._lastKey;
    }

    /**
     * @inheritDoc
     */
    next(): Promise<IteratorResult<DynamoDbResultsPage>> {
        this.lastResolved = this.lastResolved.then(() => {
            if (this.count >= (this.limit === undefined ? Infinity : this.limit)) {
                return {done: true} as IteratorResult<DynamoDbResultsPage>;
            }

            return this.getNext().then(({done, value}) => {
                    if (value && !done) {
                        this._lastKey = value.LastEvaluatedKey;
                        this._count += (value.Items || []).length;
                        this._scannedCount += (value.ScannedCount || 0);
                        this._consumedCapacity = mergeConsumedCapacities(
                            this._consumedCapacity,
                            value.ConsumedCapacity
                        );
                    }

                    return { value, done };
                })
            }
        );

        return this.lastResolved;
    }

    /**
     * @inheritDoc
     */
    return(): Promise<IteratorResult<DynamoDbResultsPage>> {
        // Prevent any further use of this iterator
        this.lastResolved = Promise.reject(new Error(
            'Iteration has been manually interrupted and may not be resumed'
        ));
        this.lastResolved.catch(() => {});

        return Promise.resolve(
            {done: true} as IteratorResult<DynamoDbResultsPage>
        );
    }

    /**
     * @inheritDoc
     */
    get scannedCount(): number {
        return this._scannedCount;
    }

    /**
     * Perform the next iteration
     */
    protected abstract getNext(): Promise<IteratorResult<DynamoDbResultsPage>>;

    protected getNextPageSize(requestedPageSize?: number): number|undefined {
        if (this.limit === undefined) {
            return requestedPageSize;
        }

        return Math.min(
            requestedPageSize === undefined ? Infinity : requestedPageSize,
            this.limit - this.count
        );
    }
}
