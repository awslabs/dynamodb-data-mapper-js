import { DynamoDbPaginatorInterface } from './DynamoDbPaginatorInterface';
import { DynamoDbResultsPage } from './DynamoDbResultsPage';
import { AttributeMap, ConsumedCapacity, Key } from 'aws-sdk/clients/dynamodb';

if (Symbol && !Symbol.asyncIterator) {
    (Symbol as any).asyncIterator = Symbol.for("__@@asyncIterator__");
}

export abstract class ItemIterator<
    Paginator extends DynamoDbPaginatorInterface
> implements AsyncIterableIterator<AttributeMap> {

    private _iteratedCount = 0;
    private lastResolved: Promise<IteratorResult<AttributeMap>> = <any>Promise.resolve();
    private lastYielded?: AttributeMap;
    private readonly pending: Array<AttributeMap> = [];

    protected constructor(
        protected readonly paginator: Paginator,
        private readonly keyProperties: Array<string>
    ) {}

    /**
     * @inheritDoc
     */
    [Symbol.asyncIterator](): AsyncIterableIterator<AttributeMap> {
        return this;
    }

    /**
     * The capacity units consumed by the Scan operation. The data returned
     * includes the total provisioned throughput consumed, along with statistics
     * for the table and any indexes involved in the operation. ConsumedCapacity
     * is only returned if the ReturnConsumedCapacity parameter was specified.
     */
    get consumedCapacity(): ConsumedCapacity|undefined {
        return this.paginator.consumedCapacity;
    }

    /**
     * The number of items that have been iterated over.
     */
    get count(): number {
        return this._iteratedCount;
    }

    next(): Promise<IteratorResult<AttributeMap>> {
        this.lastResolved = this.lastResolved.then(() => this.getNext());
        return this.lastResolved;
    }

    return(): Promise<IteratorResult<AttributeMap>> {
        // Prevent any further use of this iterator
        this.lastResolved = Promise.reject(new Error(
            'Iteration has been manually interrupted and may not be resumed'
        ));
        this.lastResolved.catch(() => {});

        // Clear the pending queue to free up memory
        this.pending.length = 0;
        return this.paginator.return().then(doneSigil);
    }

    /**
     * The number of items evaluated, before any ScanFilter is applied. A high
     * scannedCount value with few, or no, Count results indicates an
     * inefficient Scan operation. For more information, see Count and
     * ScannedCount in the Amazon DynamoDB Developer Guide.
     */
    get scannedCount(): number {
        return this.paginator.scannedCount;
    }

    protected handlePage(page: DynamoDbResultsPage): void {
        this.pending.push(...page.Items || []);
    }

    protected hasPendingItems(): boolean {
        return this.pending.length > 0;
    }

    protected lastYieldedAsKey(): Key|undefined {
        const key: Key = {};
        for (const keyAttribute of this.keyProperties) {
            // This method will only be invoked if any items have been enqueued
            // for iteration, after which `this.lastYielded` will always be set
            key[keyAttribute] = this.lastYielded![keyAttribute];
        }

        return key;
    }

    private getNext(): Promise<IteratorResult<AttributeMap>> {
        if (this.hasPendingItems()) {
            this._iteratedCount++;
            this.lastYielded = this.pending.shift();
            return Promise.resolve({
                value: this.lastYielded!,
                done: false
            });
        }

        return this.paginator.next().then(({done, value}) => {
            if (done) {
                return {done} as IteratorResult<AttributeMap>;
            }

            this.handlePage(value);
            return this.getNext();
        });
    }
}

function doneSigil() {
    return {done: true} as IteratorResult<any>;
}
