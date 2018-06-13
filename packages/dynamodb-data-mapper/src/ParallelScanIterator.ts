import { Iterator } from './Iterator';
import { ParallelScanOptions, ParallelScanState } from './namedParameters';
import { ParallelScanPaginator } from './ParallelScanPaginator';
import { ZeroArgumentsConstructor } from '@aws/dynamodb-data-marshaller';
import DynamoDB = require('aws-sdk/clients/dynamodb');

/**
 * Iterates over each item returned by a parallel DynamoDB scan until no more
 * pages are available.
 */
export class ParallelScanIterator<T> extends
    Iterator<T, ParallelScanPaginator<T>>
{
    private readonly _paginator: ParallelScanPaginator<T>;

    constructor(
        client: DynamoDB,
        itemConstructor: ZeroArgumentsConstructor<T>,
        segments: number,
        options: ParallelScanOptions & { tableNamePrefix?: string } = {}
    ) {
        const paginator = new ParallelScanPaginator(
            client,
            itemConstructor,
            segments,
            options
        );

        super(paginator);
        this._paginator = paginator;
    }

    /**
     * The `lastEvaluatedKey` attribute is not available on parallel scans. Use
     * {@link scanState} instead.
     */
    get lastEvaluatedKey() {
        return undefined;
    }

    /**
     * A snapshot of the current state of a parallel scan. May be used to resume
     * a parallel scan with a separate paginator.
     */
    get scanState(): ParallelScanState {
        let {currentSegment, scanState} = this._paginator;

        if (currentSegment !== undefined && this.hasPendingItems()) {
            scanState = [...scanState];
            scanState[currentSegment] = {
                initialized: true,
                lastEvaluatedKey: this.lastYielded
            };
        }

        return scanState;
    }
}
