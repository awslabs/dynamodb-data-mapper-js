import { DynamoDbPaginatorInterface } from './DynamoDbPaginatorInterface';
import { DynamoDbResultsPage } from './DynamoDbResultsPage';
import { mergeConsumedCapacities } from './mergeConsumedCapacities';
import { ConsumedCapacity, Key } from 'aws-sdk/clients/dynamodb';

if (Symbol && !Symbol.asyncIterator) {
    (Symbol as any).asyncIterator = Symbol.for("__@@asyncIterator__");
}

export abstract class DynamoDbPaginator implements DynamoDbPaginatorInterface {
    private _consumedCapacity?: ConsumedCapacity;
    private _count = 0;
    private _lastKey?: Key;
    private _scannedCount = 0;
    private lastResolved: Promise<IteratorResult<DynamoDbResultsPage>>
        = <any>Promise.resolve();

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
    get lastEvaluatedKey(): Key|undefined {
        return this._lastKey;
    }

    /**
     * @inheritDoc
     */
    next(): Promise<IteratorResult<DynamoDbResultsPage>> {
        this.lastResolved = this.lastResolved.then(
            () => this.getNext().then(({done, value}) => {
                if (value && !done) {
                    this._lastKey = value.LastEvaluatedKey;
                    this._count += (value.Count || 0);
                    this._scannedCount += (value.ScannedCount || 0);
                    this._consumedCapacity = mergeConsumedCapacities(
                        this._consumedCapacity,
                        value.ConsumedCapacity
                    );
                }

                return { value, done };
            })
        );

        return this.lastResolved;
    }

    /**
     * @inheritDoc
     */
    return(): Promise<IteratorResult<DynamoDbResultsPage>> {
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
}
