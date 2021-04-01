import { DynamoDbPaginator } from './DynamoDbPaginator';
import { DynamoDbResultsPage } from './DynamoDbResultsPage';
import { QueryInput } from '@aws-sdk/client-dynamodb';
import DynamoDB = require('@aws-sdk/client-dynamodb');

export class QueryPaginator extends DynamoDbPaginator {
    private nextRequest?: QueryInput;

    constructor(
        private readonly client: DynamoDB,
        input: QueryInput,
        limit?: number
    ) {
        super(limit);
        this.nextRequest = {...input};
    }

    protected getNext(): Promise<IteratorResult<DynamoDbResultsPage>> {
        if (this.nextRequest) {
            return this.client.query({
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
