# Amazon DynamoDB Batch Iteration

[![Apache 2 License](https://img.shields.io/github/license/awslabs/dynamodb-data-mapper-js.svg?style=flat)](http://aws.amazon.com/apache-2-0/)

This library provides utilities for automatically submitting arbitrarily-sized
batches of reads and writes to DynamoDB using well-formed `BatchGetItem` and
`BatchWriteItem` operations, respectively. Partial successes (i.e.,
`BatchGetItem` operations that return some responses and some unprocessed keys
or `BatchWriteItem` operations that return some unprocessed items) will retry
the unprocessed items automatically using exponential backoff.

## Getting started

### Reading batches of items

Create a `BatchGet` object, supplying an instantiated DynamoDB client from the
AWS SDK for JavaScript and an iterable of keys that you wish to retrieve. The
iterable may be synchronous (such as an array) or asynchronous (such as an
object stream wrapped with [async-iter-stream](https://github.com/calvinmetcalf/async-iter-stream)'s
`wrap` method).

```typescript
import { BatchGet } from '@aws/dynamodb-batch-iterator';
import DynamoDB = require('aws-sdk/clients/dynamodb');

const dynamoDb = new DynamoDB({region: 'us-west-2'});
const keys = [
    ['tableName', {keyProperty: {N: '0'}}],
    ['tableName', {keyProperty: {N: '1'}}],
    ['tableName', {keyProperty: {N: '2'}}],
    // etc., continuing to count up to
    ['tableName', {keyProperty: {N: '1001'}}],
];

for await (const item of new BatchGet(dynamoDb, keys)) {
    console.log(item);
}
```

The above code snippet will automatically split the provided keys into
`BatchGetItem` requests of 100 or fewer keys, and any unprocessed keys will be
automatically retried until they are handled. The above code will execute at
least 11 `BatchGetItem` operations, dependening on how many items are returned
without processing due to insufficient provisioned read capacity.

Each item yielded in the `for...await...of` loop will be a single DynamoDB
record. Iteration will stop once each key has been retrieved or an error has
been encountered.

### Writing batches of items

Create a `BatchWrite` object, supplying an instantiated DynamoDB client from the
AWS SDK for JavaScript and an iterable of write requests that you wish to
execute. The iterable may be synchronous (such as an array) or asynchronous
(such as an object stream wrapped with [async-iter-stream](https://github.com/calvinmetcalf/async-iter-stream)'s
`wrap` method).

Each write request should contain either a `DeleteRequest` key or a `PutRequest`
key as described [in the Amazon DynamoDB API reference](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_WriteRequest.html#DDB-Type-WriteRequest-DeleteRequest).

```typescript
import { BatchWrite } from '@aws/dynamodb-batch-iterator';
import DynamoDB = require('aws-sdk/clients/dynamodb');

const dynamoDb = new DynamoDB({region: 'us-west-2'});
const keys = [
    ['tableName', {DeleteRequest: {Key: {keyProperty: {N: '0'}}}}],
    ['tableName', {PutRequest: {Item: {keyProperty: {N: '1'}, otherProperty: {BOOL: false}}}}],
    ['tableName', {DeleteRequest: {Key: {keyProperty: {N: '2'}}}}],
    ['tableName', {PutRequest: {Item: {keyProperty: {N: '3'}, otherProperty: {BOOL: false}}}}],
    ['tableName', {N: '2'}],
    // etc., continuing to count up to
    ['tableName', {DeleteRequest: {Key: {keyProperty: {N: '102'}}}}],
];

for await (const item of new BatchGet(dynamoDb, keys)) {
    console.log(item);
}
```

The above code snippet will automatically split the provided keys into
`BatchWriteItem` requests of 25 or fewer write request objects, and any
unprocessed request objects will be automatically retried until they are
handled. The above code will execute at least 5 `BatchWriteItem` operations,
dependening on how many items are returned without processing due to
insufficient provisioned write capacity.

Each item yielded in the `for...await...of` loop will be a single write request
that has succeeded. Iteration will stop once each request has been handled or an
error has been encountered.
