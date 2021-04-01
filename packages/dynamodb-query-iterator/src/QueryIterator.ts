import { ItemIterator } from './ItemIterator';
import { QueryPaginator } from './QueryPaginator';
import { QueryInput } from '@aws-sdk/client-dynamodb';
import DynamoDB = require('@aws-sdk/client-dynamodb');

export class QueryIterator extends ItemIterator<QueryPaginator> {
    constructor(client: DynamoDB, input: QueryInput, limit?: number) {
        super(new QueryPaginator(client, input, limit));
    }
}
