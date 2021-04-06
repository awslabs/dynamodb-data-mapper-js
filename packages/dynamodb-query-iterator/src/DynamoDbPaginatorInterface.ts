import { DynamoDbResultsPage } from './DynamoDbResultsPage';
import { ConsumedCapacity } from '@aws-sdk/client-dynamodb';

export interface DynamoDbPaginatorInterface extends
    AsyncIterableIterator<DynamoDbResultsPage>
{
    /**
     * The capacity units consumed by the Scan operation. The data returned
     * includes the total provisioned throughput consumed, along with statistics
     * for the table and any indexes involved in the operation. ConsumedCapacity
     * is only returned if the ReturnConsumedCapacity parameter was specified.
     */
    readonly consumedCapacity: ConsumedCapacity|undefined;

    /**
     * The number of items in the results yielded.
     */
    readonly count: number;

    /**
     * The number of items evaluated, before any ScanFilter is applied. A high
     * scannedCount value with few, or no, Count results indicates an
     * inefficient Scan operation. For more information, see Count and
     * ScannedCount in the Amazon DynamoDB Developer Guide.
     */
    readonly scannedCount: number;

    /**
     * @inheritDoc
     */
    return(): Promise<IteratorResult<DynamoDbResultsPage>>;
}
