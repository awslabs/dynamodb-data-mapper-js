import { ItemIterator } from './ItemIterator';
import { QueryPaginator } from './QueryPaginator';
import { Key, QueryInput } from 'aws-sdk/clients/dynamodb';
import DynamoDB = require('aws-sdk/clients/dynamodb');

export class QueryIterator extends ItemIterator<QueryPaginator> {
    private finalKey?: Key;

    constructor(
        client: DynamoDB,
        input: QueryInput,
        keyProperties: Array<string>
    ) {
        super(new QueryPaginator(client, input), keyProperties);
    }

    /**
     * @inheritDoc
     */
    return() {
        this.finalKey = this.lastEvaluatedKey;
        return super.return();
    }

    get lastEvaluatedKey(): Key|undefined {
        if (this.finalKey) {
            return this.finalKey;
        }

        return this.hasPendingItems()
            ? this.lastYieldedAsKey()
            : this.paginator.lastEvaluatedKey;
    }
}
