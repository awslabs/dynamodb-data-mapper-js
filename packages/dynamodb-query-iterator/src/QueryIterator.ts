import { ItemIterator } from './ItemIterator';
import { QueryPaginator } from './QueryPaginator';
import { QueryInput } from 'aws-sdk/clients/dynamodb';
const DynamoDB = require('aws-sdk/clients/dynamodb');

export class QueryIterator extends ItemIterator<QueryPaginator> {
    constructor(client: DynamoDB, input: QueryInput, limit?: number) {
        super(new QueryPaginator(client, input, limit));
    }
}
