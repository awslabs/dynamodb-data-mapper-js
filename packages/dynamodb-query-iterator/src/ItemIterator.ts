import { DynamoDbPaginatorInterface } from './DynamoDbPaginatorInterface';
import {AttributeValue, ConsumedCapacity} from '@aws-sdk/client-dynamodb';

if (Symbol && !Symbol.asyncIterator) {
    (Symbol as any).asyncIterator = Symbol.for("__@@asyncIterator__");
}

export abstract class ItemIterator<
    Paginator extends DynamoDbPaginatorInterface
> implements AsyncIterableIterator<{[key: string]: AttributeValue}> {

    private _iteratedCount = 0;
    private lastResolved: Promise<IteratorResult<{[key: string]: AttributeValue}>> = <any>Promise.resolve();
    private readonly pending: Array<{[key: string]: AttributeValue}> = [];

    protected constructor(private readonly paginator: Paginator) {}

    /**
     * @inheritDoc
     */
    [Symbol.asyncIterator](): AsyncIterableIterator<{[key: string]: AttributeValue}> {
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

    /**
     * @inheritDoc
     */
    next(): Promise<IteratorResult<{[key: string]: AttributeValue}>> {
        this.lastResolved = this.lastResolved.then(() => this.getNext());
        return this.lastResolved;
    }

    /**
     * Detaches the underlying paginator from this iterator and returns it. The
     * paginator will yield arrays of unmarshalled items, with each yielded
     * array corresponding to a single call to the underlying API. As with the
     * underlying API, pages may contain a variable number of items or no items,
     * in which case an empty array will be yielded.
     *
     * Calling this method will disable further iteration.
     */
    pages(): Paginator {
        // Prevent the iterator from being used further and squelch any uncaught
        // promise rejection warnings
        this.lastResolved = Promise.reject(new Error(
            'The underlying paginator has been detached from this iterator.'
        ));
        this.lastResolved.catch(() => {});

        return this.paginator;
    }

    /**
     * @inheritDoc
     */
    return(): Promise<IteratorResult<{[key: string]: AttributeValue}>> {
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

    private getNext(): Promise<IteratorResult<{[key: string]: AttributeValue}>> {
        if (this.pending.length > 0) {
            this._iteratedCount++;
            return Promise.resolve({
                value: this.pending.shift()!,
                done: false
            });
        }

        return this.paginator.next().then(({done, value}) => {
            if (done) {
                return {done} as IteratorResult<{[key: string]: AttributeValue}>;
            }

            this.pending.push(...value.Items || []);
            return this.getNext();
        });
    }
}

function doneSigil() {
    return {done: true} as IteratorResult<any>;
}
