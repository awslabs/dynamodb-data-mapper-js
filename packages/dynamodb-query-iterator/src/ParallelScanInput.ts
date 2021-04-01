import { ScanInput } from '@aws-sdk/client-dynamodb';

export interface ParallelScanInput extends ScanInput {
    /**
     * The exclusive start key for a particular scan segment must be coordinated
     * across all active segments. To resume a previously suspending parallel
     * scan, provide a `scanState` initializer when creating a
     * ParallelScanPaginator.
     */
    ExclusiveStartKey?: undefined;

    /**
     * The segment identifier for each request will be assigned by the parallel
     * scan orchestrator.
     */
    Segment?: undefined;

    /**
     * @inheritDoc
     *
     * `TotalSegments` **MUST** be specified when initializing or resuming a
     * parallel scan.
     */
    TotalSegments: number;
}
