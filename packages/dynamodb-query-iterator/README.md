# Amazon DynamoDB Query and Scan Iteration

[![Apache 2 License](https://img.shields.io/github/license/awslabs/dynamodb-data-mapper-js.svg?style=flat)](http://aws.amazon.com/apache-2-0/)

This library provides utilities for automatically iterating over all DynamoDB
records returned by a query or scan operation using [async iterables](https://tc39.github.io/ecma262/#sec-asynciterable-interface).
Each iterator and paginator included in this package automatically tracks 
DynamoDB metadata and supports resuming iteration from any point within a full
query or scan.

## Paginators

Paginators are asynchronous iterables that yield each page of results returned 
by a DynamoDB `query` or `scan` operation. For sequential paginators, each 
invocation of the `next` method corresponds to an invocation of the underlying 
API operation until all no more pages are available.

### QueryPaginator

Retrieves all pages of a DynamoDB `query` in order.

#### Example usage

```typescript
import { QueryPaginator } from '@aws/dynamodb-query-iterator';
import DynamoDB = require('aws-sdk/clients/dynamodb');

const paginator = new QueryPaginator(
    new DynamoDB({region: 'us-west-2'}),
    {
        TableName: 'my_table',
        KeyConditionExpression: 'partitionKey = :value',
        ExpressionAttributeValues: {
            ':value': {S: 'foo'}
        },
        ReturnConsumedCapacity: 'INDEXES'
    }
);

for await (const page of paginator) {
    // do something with `page`
}

// Inspect the total number of items yielded
console.log(paginator.count);

// Inspect the total number of items scanned by this operation
console.log(paginator.scannedCount);

// Inspect the capacity consumed by this operation 
// This will only be available if `ReturnConsumedCapacity` was set on the input
console.log(paginator.consumedCapacity);
```

#### Suspending and resuming queries

You can suspend any running query from within the `for` loop by using the 
`break` keyword. If there are still pages that have not been fetched, the 
`lastEvaluatedKey` property of paginator will be defined. This can be provided 
as the `ExclusiveStartKey` for another `QueryPaginator` instance:

```typescript
import { QueryPaginator } from '@aws/dynamodb-query-iterator';
import { QueryInput } from 'aws-sdk/clients/dynamodb';
import DynamoDB = require('aws-sdk/clients/dynamodb');

const dynamoDb = new DynamoDB({region: 'us-west-2'});
const input: QueryInput = {
    TableName: 'my_table',
    KeyConditionExpression: 'partitionKey = :value',
    ExpressionAttributeValues: {
        ':value': {S: 'foo'}
    },
    ReturnConsumedCapacity: 'INDEXES'
};

const paginator = new QueryPaginator(dynamoDb, input);

for await (const page of paginator) {
    // do something with the first page of results
    break
}

for await (const page of new QueryPaginator(dynamoDb, {
    ...input,
    ExclusiveStartKey: paginator.lastEvaluatedKey
})) {
    // do something with the remaining pages
}
```

Suspending and resuming the same paginator instance is not supported.

### ScanPaginator

Retrieves all pages of a DynamoDB `scan` in order.

#### Example usage

```typescript
import { ScanPaginator } from '@aws/dynamodb-query-iterator';
import DynamoDB = require('aws-sdk/clients/dynamodb');

const paginator = new ScanPaginator(
    new DynamoDB({region: 'us-west-2'}),
    {
        TableName: 'my_table',
        ReturnConsumedCapacity: 'INDEXES'
    }
);

for await (const page of paginator) {
    // do something with `page`
}

// Inspect the total number of items yielded
console.log(paginator.count);

// Inspect the total number of items scanned by this operation
console.log(paginator.scannedCount);

// Inspect the capacity consumed by this operation 
// This will only be available if `ReturnConsumedCapacity` was set on the input
console.log(paginator.consumedCapacity);
```

#### Suspending and resuming scans

You can suspend any running scan from within the `for` loop by using the `break`
keyword. If there are still pages that have not been fetched, the
`lastEvaluatedKey` property of paginator will be defined. This can be provided
as the `ExclusiveStartKey` for another `ScanPaginator` instance:

```typescript
import { ScanPaginator } from '@aws/dynamodb-query-iterator';
import { ScanInput } from 'aws-sdk/clients/dynamodb';
import DynamoDB = require('aws-sdk/clients/dynamodb');

const dynamoDb = new DynamoDB({region: 'us-west-2'});
const input: ScanInput = {
    TableName: 'my_table',
    ReturnConsumedCapacity: 'INDEXES'
};

const paginator = new ScanPaginator(dynamoDb, input);

for await (const page of paginator) {
    // do something with the first page of results
    break
}

for await (const page of new ScanPaginator(dynamoDb, {
    ...input,
    ExclusiveStartKey: paginator.lastEvaluatedKey
})) {
    // do something with the remaining pages
}
```

Suspending and resuming the same paginator instance is not supported.

### ParallelScanPaginator

Retrieves all pages of a DynamoDB `scan` utilizing a configurable number of scan
segments that operate in parallel. When performing a parallel scan, you must
specify the total number of segments you wish to use, and neither an 
`ExclusiveStartKey` nor a `Segment` identifier may be included with the input
provided.

#### Example usage

```typescript
import { ParallelScanPaginator } from '@aws/dynamodb-query-iterator';
import DynamoDB = require('aws-sdk/clients/dynamodb');

const paginator = new ParallelScanPaginator(
    new DynamoDB({region: 'us-west-2'}),
    {
        TableName: 'my_table',
        TotalSegments: 4,
        ReturnConsumedCapacity: 'INDEXES'
    }
);

for await (const page of paginator) {
    // do something with `page`
}

// Inspect the total number of items yielded
console.log(paginator.count);

// Inspect the total number of items scanned by this operation
console.log(paginator.scannedCount);

// Inspect the capacity consumed by this operation 
// This will only be available if `ReturnConsumedCapacity` was set on the input
console.log(paginator.consumedCapacity);
```

#### Suspending and resuming parallel scans

You can suspend any running scan from within the `for` loop by using the `break`
keyword. If there are still pages that have not been fetched, the `scanState`
property of interrupted paginator can be provided to the constructor of another
`ParallelScanPaginator` instance:

```typescript
import { 
    ParallelScanInput,
    ParallelScanPaginator,
} from '@aws/dynamodb-query-iterator';
import DynamoDB = require('aws-sdk/clients/dynamodb');

const client = new DynamoDB({region: 'us-west-2'});
const input: ParallelScanInput = {
    TableName: 'my_table',
    TotalSegments: 4,
    ReturnConsumedCapacity: 'INDEXES'
};

const paginator = new ParallelScanPaginator(client, input);

for await (const page of paginator) {
    // do something with the first page of results
    break
}

for await (const page of new ParallelScanPaginator(
    client,
    input,
    paginator.scanState
)) {
    // do something with the remaining pages
}
```

Suspending and resuming the same paginator instance is not supported.


## Iterators

Iterators are asynchronous iterables that yield each of record returned by a 
DynamoDB `query` or `scan` operation. Each invocation of the `next` method may
invoke the underlying API operation until all no more pages are available.

### QueryIterator

Retrieves all records of a DynamoDB `query` in order.

#### Example usage

```typescript
import { QueryIterator } from '@aws/dynamodb-query-iterator';
import DynamoDB = require('aws-sdk/clients/dynamodb');

const iterator = new QueryIterator(
    new DynamoDB({region: 'us-west-2'}),
    {
        TableName: 'my_table',
        KeyConditionExpression: 'partitionKey = :value',
        ExpressionAttributeValues: {
            ':value': {S: 'foo'}
        },
        ReturnConsumedCapacity: 'INDEXES'
    },
    ['partitionKey']
);

for await (const record of iterator) {
    // do something with `record`
}

// Inspect the total number of items yielded
console.log(iterator.count);

// Inspect the total number of items scanned by this operation
console.log(iterator.scannedCount);

// Inspect the capacity consumed by this operation 
// This will only be available if `ReturnConsumedCapacity` was set on the input
console.log(iterator.consumedCapacity);
```

### ScanIterator

Retrieves all records of a DynamoDB `scan` in order.

#### Example usage

```typescript
import { ScanIterator } from '@aws/dynamodb-query-iterator';
import DynamoDB = require('aws-sdk/clients/dynamodb');

const iterator = new ScanIterator(
    new DynamoDB({region: 'us-west-2'}),
    {
        TableName: 'my_table',
        ReturnConsumedCapacity: 'INDEXES'
    },
    ['partitionKey', 'sortKey']
);

for await (const record of iterator) {
    // do something with `record`
}

// Inspect the total number of items yielded
console.log(iterator.count);

// Inspect the total number of items scanned by this operation
console.log(iterator.scannedCount);

// Inspect the capacity consumed by this operation 
// This will only be available if `ReturnConsumedCapacity` was set on the input
console.log(iterator.consumedCapacity);
```

### ParallelScanIterator

Retrieves all pages of a DynamoDB `scan` utilizing a configurable number of scan
segments that operate in parallel. When performing a parallel scan, you must
specify the total number of segments you wish to use, and neither an 
`ExclusiveStartKey` nor a `Segment` identifier may be included with the input
provided.

#### Example usage

```typescript
import { ParallelScanIterator} from '@aws/dynamodb-query-iterator';
import DynamoDB = require('aws-sdk/clients/dynamodb');

const iterator = new ParallelScanIterator(
    new DynamoDB({region: 'us-west-2'}),
    {
        TableName: 'my_table',
        TotalSegments: 4,
        ReturnConsumedCapacity: 'INDEXES'
    },
    ['partitionKey']
);

for await (const record of iterator) {
    // do something with `record`
}

// Inspect the total number of items yielded
console.log(iterator.count);

// Inspect the total number of items scanned by this operation
console.log(iterator.scannedCount);

// Inspect the capacity consumed by this operation 
// This will only be available if `ReturnConsumedCapacity` was set on the input
console.log(iterator.consumedCapacity);
```


