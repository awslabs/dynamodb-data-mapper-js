import { Paginator as AbstractPaginator } from './Paginator';
import { ConsumedCapacity } from 'aws-sdk/clients/dynamodb';

require('./asyncIteratorSymbolPolyfill');

export abstract class Iterator<
    T,
    Paginator extends AbstractPaginator<T, any>
> implements AsyncIterableIterator<T> {
    private _count = 0;
    private finalKey?: Partial<T>;
    private lastResolved: Promise<IteratorResult<T>> = Promise.resolve() as any;
    private readonly pending: Array<T> = [];

    protected lastYielded?: T;

    protected constructor(private readonly paginator: Paginator) {}

    /**
     * @inheritDoc
     */
    [Symbol.asyncIterator]() {
        return this;
    }

    /**
     * @inheritDoc
     */
    next(): Promise<IteratorResult<T>> {
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

        this.finalKey = this.lastEvaluatedKey;

        return this.paginator;
    }

    /**
     * @inheritDoc
     */
    return(): Promise<IteratorResult<T>> {
        this.finalKey = this.lastEvaluatedKey;

        // Prevent any further use of this iterator
        this.lastResolved = Promise.reject(new Error(
            'Iteration has been manually interrupted and may not be resumed'
        ));
        this.lastResolved.catch(() => {});

        // Empty the pending queue to free up memory
        this.pending.length = 0;
        return this.paginator.return() as any;
    }

    /**
     * Retrieve the reported capacity consumed by this iterator. Will be
     * undefined unless returned consumed capacity is requested.
     */
    get consumedCapacity(): ConsumedCapacity|undefined {
        return this.paginator.consumedCapacity;
    }

    /**
     * Retrieve the number of items yielded thus far by this iterator.
     */
    get count() {
        return this._count;
    }

    /**
     * Retrieve the last reported `LastEvaluatedKey`, unmarshalled according to
     * the schema used by this iterator.
     */
    get lastEvaluatedKey(): Partial<T>|undefined {
        if (this.finalKey) {
            return this.finalKey;
        }

        if (this.hasPendingItems() && this.lastYielded) {
            return this.lastYielded;
        }

        return this.paginator.lastEvaluatedKey;
    }

    /**
     * Retrieve the number of items scanned thus far during the execution of
     * this iterator. This number should be the same as {@link count} unless a
     * filter expression was used.
     */
    get scannedCount() {
        return this.paginator.scannedCount;
    }

    protected hasPendingItems() {
        return this.pending.length > 0;
    }

    private async getNext(): Promise<IteratorResult<T>> {
        if (this.pending.length > 0) {
            this.lastYielded = this.pending.shift()!;
            this._count++;
            return {
                done: false,
                value: this.lastYielded
            }
        }

        return this.paginator.next().then(({value = [], done}) => {
            if (!done) {
                this.pending.push(...value);
                return this.getNext();
            }

            this.lastYielded = undefined;
            return {done: true} as IteratorResult<T>;
        });
    }
}
