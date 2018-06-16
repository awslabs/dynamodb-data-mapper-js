import { ItemIterator } from './ItemIterator';
import { ScanPaginator } from './ScanPaginator';
import { ScanInput } from 'aws-sdk/clients/dynamodb';
import DynamoDB = require('aws-sdk/clients/dynamodb');

export class ScanIterator extends ItemIterator<ScanPaginator> {
    constructor(client: DynamoDB, input: ScanInput, limit?: number) {
        super(new ScanPaginator(client, input, limit));
    }
}
