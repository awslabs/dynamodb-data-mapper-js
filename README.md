# Amazon DynamoDB DataMapper For JavaScript

[![Apache 2 License](https://img.shields.io/github/license/awslabs/dynamodb-data-mapper-js.svg?style=flat)](http://aws.amazon.com/apache-2-0/)

This repository hosts several packages that collectively make up an object to
document mapper for JavaScript applications using Amazon DynamoDB.

## Getting started

[The `@aws/dynamodb-data-mapper`package](packages/dynamodb-data-mapper) provides
a simple way to persist and load an application's domain objects to and from
Amazon DynamoDB. When used together with the decorators provided by [the
`@aws/dynamodb-data-mapper-annotations` package](packages/dynamodb-data-mapper-annotations),
you can describe the relationship between a class and its representation in
DynamoDB by adding a few decorators:

```typescript
import {DataMapper} from '@aws/dynamodb-data-mapper';
import {
    attribute,
    hashKey,
    rangeKey,
    table,
} from '@aws/dynamodb-data-mapper-annotations';

@table('table_name')
class MyDomainClass {
    @hashKey()
    id: string;

    @rangeKey({defaultProvider: () => new Date()})
    createdAt: Date;

    @attribute()
    completed?: boolean;
}

// Now you can save instances of this item to DynamoDB
const myDomainObject = new MyDomainClass();
myDomainObject.id = 'id';
const mapper = new DataMapper()
mapper.put(myDomainObject);
```

Please refer to the individual packages for more detailed documentation.

## Constituent packages

* [Amazon DynamoDB Automarshaller](packages/dynamodb-auto-marshaller/)
* [Amazon DynamoDB DataMapper](packages/dynamodb-data-mapper/)
* [Amazon DynamoDB DataMapper Annotations](packages/dynamodb-data-mapper-annotations/)
* [Amazon DynamoDB Data Marshaller](packages/dynamodb-data-marshaller/)
* [Amazon DynamoDB Expressions](packages/dynamodb-expressions/)
