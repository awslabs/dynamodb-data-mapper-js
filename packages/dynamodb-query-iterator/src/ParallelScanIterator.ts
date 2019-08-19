import { ItemIterator } from './ItemIterator';
import { ParallelScanInput } from './ParallelScanInput';
import {
    ParallelScanPaginator,
    ParallelScanState,
} from './ParallelScanPaginator';
const DynamoDB = require('aws-sdk/clients/dynamodb');

export class ParallelScanIterator extends ItemIterator<ParallelScanPaginator> {
    constructor(
        client: DynamoDB,
        input: ParallelScanInput,
        scanState?: ParallelScanState
    ) {
        super(new ParallelScanPaginator(client, input, scanState));
    }
}
