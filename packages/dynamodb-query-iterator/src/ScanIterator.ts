import { ItemIterator } from './ItemIterator';
import { ScanPaginator } from './ScanPaginator';
import { Key, ScanInput } from 'aws-sdk/clients/dynamodb';
import DynamoDB = require('aws-sdk/clients/dynamodb');

export class ScanIterator extends ItemIterator<ScanPaginator> {
    private finalKey?: Key;

    constructor(
        client: DynamoDB,
        input: ScanInput,
        keyProperties: Array<string>
    ) {
        super(new ScanPaginator(client, input), keyProperties);
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
