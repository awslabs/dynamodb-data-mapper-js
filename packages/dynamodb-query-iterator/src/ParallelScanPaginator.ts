import { DynamoDbPaginatorInterface } from './DynamoDbPaginatorInterface';
import { DynamoDbResultsPage } from './DynamoDbResultsPage';
import { mergeConsumedCapacities } from './mergeConsumedCapacities';
import { ParallelScanInput } from './ParallelScanInput';
import { ScanPaginator } from './ScanPaginator';
import { ConsumedCapacity, Key } from 'aws-sdk/clients/dynamodb';
import DynamoDB = require('aws-sdk/clients/dynamodb');

/**
 * Pagination state for a scan segment for which the first page has not yet been
 * retrieved.
 */
export interface UninitializedScanState {
    initialized: false;
    LastEvaluatedKey?: undefined;
}

/**
 * Pagination state for a scan segment for which one or more pages have been
 * retrieved. If `LastEvaluatedKey` is defined, there are more pages to fetch;
 * otherwise, all pages for this segment have been returned.
 */
export interface InitializedScanState {
    initialized: true;
    LastEvaluatedKey?: Key;
}

export type ScanState = UninitializedScanState|InitializedScanState;

/**
 * ParallelScanState is represented as an array whose length is equal to the
 * number of segments being scanned independently, with each segment's state
 * being stored at the array index corresponding to its segment number.
 *
 * Segment state is represented with a tagged union with the following keys:
 *   - `initialized` -- whether the first page of results has been retrieved
 *   - `LastEvaluatedKey` -- the key to provide (if any) when requesting the
 *      next page of results.
 *
 * If `LastEvaluatedKey` is undefined and `initialized` is true, then all pages
 * for the given segment have been returned.
 */
export type ParallelScanState = Array<ScanState>;

if (Symbol && !Symbol.asyncIterator) {
    (Symbol as any).asyncIterator = Symbol.for("__@@asyncIterator__");
}

export class ParallelScanPaginator implements DynamoDbPaginatorInterface {
    private readonly _scanState: ParallelScanState;
    private readonly iterators: Array<ScanPaginator>;
    private readonly pending: Array<PendingResult> = [];
    private lastResolved: Promise<
        IteratorResult<DynamoDbResultsPage & {segment: number}>
    > = Promise.resolve() as any;

    constructor(
        client: DynamoDB,
        input: ParallelScanInput,
        scanState: ParallelScanState = nullScanState(input.TotalSegments)
    ) {
        const { TotalSegments } = input;

        if (scanState.length !== TotalSegments) {
            throw new Error(
                `Parallel scan state must have a length equal to the number of `
                    + `scan segments. Expected an array of ${TotalSegments} but`
                    + `received an array with ${scanState.length} elements.`
            );
        }

        this.iterators = new Array(TotalSegments);
        for (let i = 0; i < TotalSegments; i++) {
            const iterator = new ScanPaginator(
                client,
                {
                    ...input,
                    Segment: i,
                    ExclusiveStartKey: scanState[i].LastEvaluatedKey,
                }
            );
            this.iterators[i] = iterator;

            // If the segment has not been initialized or a pagination token has
            // been received, request the next page.
            if (!scanState[i].initialized || scanState[i].LastEvaluatedKey) {
                this.refillPending(iterator, i);
            }
        }

        this._scanState = [...scanState];
    }

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
        return this.iterators.reduce(
            (merged: ConsumedCapacity|undefined, paginator) => mergeConsumedCapacities(
                merged,
                paginator.consumedCapacity
            ),
            undefined
        )
    }

    /**
     * @inheritDoc
     */
    get count(): number {
        return this.iterators.reduce(
            (sum, paginator) => sum + paginator.count,
            0
        );
    }

    /**
     * @inheritDoc
     */
    next(): Promise<IteratorResult<DynamoDbResultsPage & {segment: number}>> {
        this.lastResolved = this.lastResolved.then(() => this.getNext());
        return this.lastResolved;
    }

    private async getNext(): Promise<IteratorResult<DynamoDbResultsPage & {segment: number}>> {
        if (this.pending.length === 0) {
            return doneSigil();
        }

        // Grab the next available result from any segment.
        const {
            iterator,
            result: {value, done},
            segment,
        } = await Promise.race(this.pending.map(pending => pending.result));

        // Update the scan state for this segment. This will either be the last
        // evaluated key (for an unfinished segment) or undefined (for a
        // completed segment).
        this._scanState[segment] = {
            initialized: true,
            LastEvaluatedKey: value && value.LastEvaluatedKey,
        };

        // Remove the result from the pending set.
        for (let i = this.pending.length - 1; i >= 0; i--) {
            if (this.pending[i].iterator === iterator) {
                this.pending.splice(i, 1);
            }
        }

        // If the iterator is not finished, add its next result to the pending
        // set.
        if (!done) {
            this.refillPending(iterator, segment);
            return {
                value: value ? {...value, segment} : value,
                done,
            };
        } else {
            // If a segment has finished but there are still outstanding
            // requests, recur. A done sigil will be returned when the pending
            // queue is empty.
            return this.getNext();
        }
    }

    /**
     * @inheritDoc
     */
    async return(): Promise<IteratorResult<DynamoDbResultsPage>> {
        this.pending.length = 0;
        return Promise.all(this.iterators.map(iterator => iterator.return()))
            .then(doneSigil);
    }

    /**
     * @inheritDoc
     */
    get scannedCount(): number {
        return this.iterators.reduce(
            (sum, paginator) => sum + paginator.scannedCount,
            0
        );
    }

    /**
     * A snapshot of the current state of a parallel scan. May be used to resume
     * a parallel scan with a separate paginator.
     */
    get scanState(): ParallelScanState {
        return [...this._scanState];
    }

    private refillPending(iterator: ScanPaginator, segment: number): void {
        // Use .push to reorder segments within the array of pending results.
        // Promise.race will iterate over the array of pending results until a
        // resolved promise is found and therefore will naturally favor promises
        // towards the head of the queue. Removing resolved segments and sending
        // them to the back of the line will keep this implementation detail
        // from creating hot and cold scan segments.
        this.pending.push({
            iterator: iterator,
            result: iterator.next()
                .then(result => ({iterator, result, segment})),
        });
    }
}

function doneSigil() {
    return {done: true} as IteratorResult<any>;
}

/**
 * `Array.prototype.fill` is not available in IE, so a loop is used instead
 */
function nullScanState(length: number): ParallelScanState {
    const target: ParallelScanState = new Array(length);
    for (let i = 0; i < length; i++) {
        target[i] = {initialized: false};
    }

    return target;
}

interface PendingResult {
    iterator: ScanPaginator;
    result: Promise<{
        iterator: ScanPaginator;
        result: IteratorResult<DynamoDbResultsPage>;
        segment: number;
    }>;
}
