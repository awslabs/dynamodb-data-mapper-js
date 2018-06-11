import { DynamoDbResultsPage } from './DynamoDbResultsPage';
import { ItemIterator } from './ItemIterator';
import { ParallelScanInput } from './ParallelScanInput';
import {
    ParallelScanPaginator,
    ParallelScanState,
} from './ParallelScanPaginator';
import DynamoDB = require('aws-sdk/clients/dynamodb');

export class ParallelScanIterator extends ItemIterator<ParallelScanPaginator> {
    private currentSegment?: number;
    private finalState?: ParallelScanState;

    constructor(
        client: DynamoDB,
        input: ParallelScanInput,
        keyProperties: Array<string>,
        scanState?: ParallelScanState
    ) {
        super(
            new ParallelScanPaginator({client, input, scanState}),
            keyProperties
        );
    }

    /**
     * @inheritDoc
     */
    return() {
        this.finalState = this.scanState;
        return super.return();
    }

    /**
     * A snapshot of the current state of a parallel scan. May be used to resume
     * a parallel scan with a separate iterator.
     */
    get scanState(): ParallelScanState {
        if (this.finalState) {
            return this.finalState;
        }

        let { scanState } = this.paginator;

        if (this.currentSegment !== undefined && this.hasPendingItems()) {
            scanState = [...scanState];
            scanState[this.currentSegment!] = {
                initialized: true,
                LastEvaluatedKey: this.lastYieldedAsKey()
            };
        }

        return scanState;
    }

    protected handlePage(page: DynamoDbResultsPage & {segment: number}): void {
        this.currentSegment = page.segment;
        super.handlePage(page);
    }
}
