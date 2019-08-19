import { DynamoDbPaginator } from './DynamoDbPaginator';
import { DynamoDbResultsPage } from './DynamoDbResultsPage';
import { ScanInput } from 'aws-sdk/clients/dynamodb';
const DynamoDB = require('aws-sdk/clients/dynamodb');

export class ScanPaginator extends DynamoDbPaginator {
    private nextRequest?: ScanInput;

    constructor(
        private readonly client: DynamoDB,
        input: ScanInput,
        limit?: number
    ) {
        super(limit);
        this.nextRequest = {
            ...input,
            Limit: this.getNextPageSize(input.Limit),
        };
    }

    protected getNext(): Promise<IteratorResult<DynamoDbResultsPage>> {
        if (this.nextRequest) {
            return this.client.scan({
                ...this.nextRequest,
                Limit: this.getNextPageSize(this.nextRequest.Limit)
            })
                .promise()
                .then(output => {
                    if (this.nextRequest && output.LastEvaluatedKey) {
                        this.nextRequest = {
                            ...this.nextRequest,
                            ExclusiveStartKey: output.LastEvaluatedKey
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
