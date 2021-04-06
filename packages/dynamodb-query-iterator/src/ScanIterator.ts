import { ItemIterator } from './ItemIterator';
import { ScanPaginator } from './ScanPaginator';
import {DynamoDB, ScanInput} from '@aws-sdk/client-dynamodb';

export class ScanIterator extends ItemIterator<ScanPaginator> {
    constructor(client: DynamoDB, input: ScanInput, limit?: number) {
        super(new ScanPaginator(client, input, limit));
    }
}
