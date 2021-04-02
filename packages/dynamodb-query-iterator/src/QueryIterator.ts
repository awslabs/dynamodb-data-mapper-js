import { ItemIterator } from './ItemIterator';
import { QueryPaginator } from './QueryPaginator';
import {DynamoDB, QueryInput} from '@aws-sdk/client-dynamodb';

export class QueryIterator extends ItemIterator<QueryPaginator> {
    constructor(client: DynamoDB, input: QueryInput, limit?: number) {
        super(new QueryPaginator(client, input, limit));
    }
}
