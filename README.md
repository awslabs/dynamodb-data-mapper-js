# Amazon DynamoDB DataMapper For JavaScript

[![Apache 2 License](https://img.shields.io/github/license/awslabs/dynamodb-data-mapper-js.svg?style=flat)](http://aws.amazon.com/apache-2-0/)

This repository hosts several packages that collectively make up an object to
document mapper for JavaScript applications using Amazon DynamoDB.

## Getting started

[The `@aws/dynamodb-data-mapper` package](packages/dynamodb-data-mapper) provides
a simple way to persist and load an application's domain objects to and from
Amazon DynamoDB. When used together with the decorators provided by [the
`@aws/dynamodb-data-mapper-annotations` package](packages/dynamodb-data-mapper-annotations),
you can describe the relationship between a class and its representation in
DynamoDB by adding a few decorators:

```typescript
import {
    attribute,
    hashKey,
    rangeKey,
    table,
} from '@aws/dynamodb-data-mapper-annotations';

@table('table_name')
class MyDomainObject {
    @hashKey()
    id: string;

    @rangeKey({defaultProvider: () => new Date()})
    createdAt: Date;

    @attribute()
    completed?: boolean;
}
```

With domain classes defined, you can interact with records in DynamoDB via an
instance of `DataMapper`:

```typescript
import {DataMapper} from '@aws/dynamodb-data-mapper';
const DynamoDB = require('aws-sdk/clients/dynamodb');

const mapper = new DataMapper({
    client: new DynamoDB({region: 'us-west-2'}), // the SDK client used to execute operations
    tableNamePrefix: 'dev_' // optionally, you can provide a table prefix to keep your dev and prod tables separate
});
```

### Supported operations

Using the `mapper` object and `MyDomainObject` class defined above, you can
perform the following operations:

#### `put`

Creates (or overwrites) an item in the table

```typescript
const toSave = Object.assign(new MyDomainObject, {id: 'foo'});
mapper.put(toSave).then(objectSaved => {
    // the record has been saved
});
```

#### `get`

Retrieves an item from DynamoDB

```typescript
mapper.get(Object.assign(new MyDomainObject, {id: 'foo', createdAt: new Date(946684800000)}))
    .then(myItem => {
        // the item was found
    })
    .catch(err => {
        // the item was not found
    })
```

**NB:** The promise returned by the mapper will be rejected with an
`ItemNotFoundException` if the item sought is not found.

#### `update`

Updates an item in the table

```typescript
const myItem = await mapper.get(Object.assign(
    new MyDomainObject,
    {id: 'foo', createdAt: new Date(946684800000)}
));
myItem.completed = true;

await mapper.update(myItem);
```

#### `delete`

Removes an item from the table

```typescript
await mapper.delete(Object.assign(
    new MyDomainObject,
    {id: 'foo', createdAt: new Date(946684800000)}
));
```

#### `scan`

Lists the items in a table or index

```typescript
for await (const item of mapper.scan(MyDomainObject)) {
    // individual items will be yielded as the scan is performed
}

// Optionally, scan an index instead of the table:
for await (const item of mapper.scan(MyDomainObject, {indexName: 'myIndex'})) {
    // individual items will be yielded as the scan is performed
}
```

#### `query`

Finds a specific item (or range of items) in a table or index

```typescript
for await (const foo of mapper.query(MyDomainObject, {id: 'foo'})) {
    // individual items with a hash key of "foo" will be yielded as the query is performed
}
```

#### Batch operations

The mapper also supports batch operations. Under the hood, the batch will
automatically be split into chunks that fall within DynamoDB's limits (25 for
`batchPut` and `batchDelete`, 100 for `batchGet`). The items can belong to any
number of tables, and exponential backoff for unprocessed items is handled
automatically.

##### `batchPut`

Creates (or overwrites) multiple items in the table

```typescript
const toSave = [
    Object.assign(new MyDomainObject, {id: 'foo', completed: false}),
    Object.assign(new MyDomainObject, {id: 'bar', completed: false})
];
for await (const persisted of mapper.batchPut(toSave)) {
    // items will be yielded as they are successfully written
}
```

##### `batchGet`

Fetches multiple items from the table

```typescript
const toGet = [
    Object.assign(new MyDomainObject, {id: 'foo', createdAt: new Date(946684800000)}),
    Object.assign(new MyDomainObject, {id: 'bar', createdAt: new Date(946684800001)})
];
for await (const found of mapper.batchGet(toGet)) {
    // items will be yielded as they are successfully retrieved
}
```

**NB:** Only items that exist in the table will be retrieved. If a key is not
found, it will be omitted from the result.

##### `batchDelete`

Removes multiple items from the table

```typescript
const toRemove = [
    Object.assign(new MyDomainObject, {id: 'foo', createdAt: new Date(946684800000)}),
    Object.assign(new MyDomainObject, {id: 'bar', createdAt: new Date(946684800001)})
];
for await (const found of mapper.batchDelete(toRemove)) {
    // items will be yielded as they are successfully removed
}
```

#### Operations with Expressions

##### Aplication example

```js
import {
    AttributePath,
    FunctionExpression,
    UpdateExpression,
} from '@aws/dynamodb-expressions';

const expr = new UpdateExpression();

// given the anotation bellow
@table('tableName')
class MyRecord {
    @hashKey()
    email?: string;

    @attribute()
    passwordHash?: string;

    @attribute()
    passwordSalt?: string;

    @attribute()
    verified?: boolean;

    @attribute()
    verifyToken?: string;
}

// you make a mapper operation as follows
const aRecord = Object.assign(new MyRecord(), {
    email,
    passwordHash: password,
    passwordSalt: salt,
    verified: false,
    verifyToken: token,
});
mapper.put(aRecord, { 
    condition: new FunctionExpression('attribute_not_exists', new AttributePath('email') 
}).then( /* result handler */ );
``` 

#### Table lifecycle operations

##### `createTable`

Creates a table for the mapped class and waits for it to be initialized:

```typescript
mapper.createTable(MyDomainObject, {readCapacityUnits: 5, writeCapacityUnits: 5})
    .then(() => {
        // the table has been provisioned and is ready for use!
    })
```

##### `ensureTableExists`

Like `createTable`, but only creates the table if it doesn't already exist:

```typescript
mapper.ensureTableExists(MyDomainObject, {readCapacityUnits: 5, writeCapacityUnits: 5})
    .then(() => {
        // the table has been provisioned and is ready for use!
    })
```

##### `deleteTable`

Deletes the table for the mapped class and waits for it to be removed:

```typescript
await mapper.deleteTable(MyDomainObject)
```

##### `ensureTableNotExists`

Like `deleteTable`, but only deletes the table if it exists:

```typescript
await mapper.ensureTableNotExists(MyDomainObject)
```

## Constituent packages

The DataMapper is developed as a monorepo using [`lerna`](https://github.com/lerna/lerna).
More detailed documentation about the mapper's constituent packages is available
by viewing those packages directly.

* [Amazon DynamoDB Automarshaller](packages/dynamodb-auto-marshaller/)
* [Amazon DynamoDB Batch Iterator](packages/dynamodb-batch-iterator/)
* [Amazon DynamoDB DataMapper](packages/dynamodb-data-mapper/)
* [Amazon DynamoDB DataMapper Annotations](packages/dynamodb-data-mapper-annotations/)
* [Amazon DynamoDB Data Marshaller](packages/dynamodb-data-marshaller/)
* [Amazon DynamoDB Expressions](packages/dynamodb-expressions/)
* [Amazon DynamoDB Query Iterator](packages/dynamodb-query-iterator/)
