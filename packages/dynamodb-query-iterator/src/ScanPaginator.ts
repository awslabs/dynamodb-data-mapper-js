import { DynamoDbPaginator } from './DynamoDbPaginator';
import { DynamoDbResultsPage } from './DynamoDbResultsPage';
import { ScanInput } from 'aws-sdk/clients/dynamodb';
import DynamoDB = require('aws-sdk/clients/dynamodb');

export class ScanPaginator extends DynamoDbPaginator {
    private nextRequest?: ScanInput;

    constructor(
        private readonly client: DynamoDB,
        input: ScanInput
    ) {
        super();
        this.nextRequest = {...input};
    }

    protected getNext(): Promise<IteratorResult<DynamoDbResultsPage>> {
        if (this.nextRequest) {
            return this.client.scan(this.nextRequest).promise().then(output => {
                if (this.nextRequest && output.LastEvaluatedKey) {
                    this.nextRequest = {
                        ...this.nextRequest,
                        ExclusiveStartKey: output.LastEvaluatedKey,
                    };
                } else {
                    this.nextRequest = undefined;
                }

                return Promise.resolve({
                    value: output,
                    done: false
                });
            });
        }

        return Promise.resolve(
            {done: true} as IteratorResult<DynamoDbResultsPage>
        );
    }
}
