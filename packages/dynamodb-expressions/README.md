# Amazon DynamoDB Expressions

[![Apache 2 License](https://img.shields.io/github/license/awslabs/dynamodb-data-mapper-js.svg?style=flat)](http://aws.amazon.com/apache-2-0/)

This library provides a number of abstractions designed to make dealing with
Amazon DynamoDB expressions easier and more natural for JavaScript developers.

## Attribute paths

The `AttributePath` class provides a simple way to write [DynamoDB document
paths](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html#Expressions.Attributes.NestedElements.DocumentPathExamples).
If the constructor receives a string, it will parse the path by scanning for
dots (`.`), which designate map property dereferencing and left brackets (`[`),
which designate list attribute dereferencing. For example,
`'ProductReviews.FiveStar[0].reviewer.username'` would be understood as
referring to the `username` property of the `reviewer` property of the first
element of the list stored at the `FiveStar` property of the top-level
`ProductReviews` document attribute.

If a property name contains a left bracket or dot, it may be escaped with a
backslash `\`. For example, `Product\.Reviews` would be interpreted as a single
top-level document attribute rather than as a map property access.

## Attribute values

This library will marshall values encountered using runtime type detection. If
you have a value that is already in the format expected by DynamoDB, you may
pass it to the `AttributeValue` constructor to direct other expression helpers
not to marshall the value further.

## Condition expressions

DynamoDB condition expressions may come in the form of a function call or as the
combination of values and infix operators. This library therefore defines a
`ConditionExpression` as the union of [`FunctionExpression`](#function-expressions)
and a tagged union of the expression operator types. Expressions may be compound
or simple.

### Compound expressions

These expressions envelope one or more simple expressions and are true or false
based on the value of the subexpressions they contain. The recognized compound
expressions are:

#### `And` expressions

Asserts that all of the subexpressions' conditions are satisfied.

```typescript
import {ConditionExpression} from '@aws/dynamodb-expressions';

const andExpression: ConditionExpression = {
    type: 'And',
    conditions: [
        // one or more subexpressions
    ]
};
```

#### `Or` expressions

Asserts that at least one of the subexpressions' conditions are satisfied.

```typescript
import {ConditionExpression} from '@aws/dynamodb-expressions';

const orExpression: ConditionExpression = {
    type: 'Or',
    conditions: [
        // one or more subexpressions
    ]
};
```

#### `Not` expressions

Asserts that the subexpression's condition is not satisfied.

```typescript
import {ConditionExpression} from '@aws/dynamodb-expressions';

const notExpression: ConditionExpression = {
    type: 'Not',
    condition: {
        type: 'LessThan',
        subject: 'foo',
        object: 100
    }
};
```

### Simple expressions

These expressions make an assertion about a property in a DynamoDB object known
as the expression's `subject`. The `subject` must be a string or an [attribute
path](#attribute-paths).

The particular assertion used is referred to in this library as a
`ConditionExpressionPredicate`. A predicate may be declared separately from its
`subject` but only becomes a valid expression when paired with a `subject`. The
supported condition expression predicates are:

#### `Equals` expression predicate

Creates a condition which is true if the defined `subject` is equal to the
defined `object`. For example, the following predicate object asserts that the
subject has a value of `'bar'`:

```typescript
import {
    ConditionExpression,
    ConditionExpressionPredicate,
} from '@aws/dynamodb-expressions';

let equalsExpressionPredicate: ConditionExpressionPredicate = {
    type: 'Equals',
    object: 'bar'
};

// you can also define an equality predicate with the `equals` helper method
import {equals} from '@aws/dynamodb-expressions';

equalsExpressionPredicate = equals('bar');

// combine with a subject to create a valid condition expression
const equalsExpression: ConditionExpression = {
    ...equalsExpressionPredicate,
    subject: 'foo'
};
```

`object` may be an [attribute path](#attribute-paths), an [attribute
value](#attribute-values), or another type. If the lattermost type is received,
it will be serialized using the `@aws/dynamodb-auto-marshaller` package.

#### `NotEquals` expression predicate

Creates a condition which is true if the defined `subject` is NOT equal to the
defined `object`. For example, the following predicate object asserts that the
subject does not have a value of `'bar'`:

```typescript
import {
    ConditionExpression,
    ConditionExpressionPredicate,
} from '@aws/dynamodb-expressions';

let equalsExpressionPredicate: ConditionExpressionPredicate = {
    type: 'NotEquals',
    object: 'bar'
};

// you can also define an equality predicate with the `equals` helper method
import {notEquals} from '@aws/dynamodb-expressions';

equalsExpressionPredicate = notEquals('bar');

// combine with a subject to create a valid condition expression
const equalsExpression: ConditionExpression = {
    ...equalsExpressionPredicate,
    subject: 'foo'
};
```

`object` may be an [attribute path](#attribute-paths), an [attribute
value](#attribute-values), or another type. If the lattermost type is received,
it will be serialized using the `@aws/dynamodb-auto-marshaller` package.

#### `LessThan` expression predicate

Creates a condition which is true if the defined `subject` is less than the
defined `object`. For example, the following predicate object asserts that the
subject is less than 10:

```typescript
import {
    ConditionExpression,
    ConditionExpressionPredicate,
} from '@aws/dynamodb-expressions';

let equalsExpressionPredicate: ConditionExpressionPredicate = {
    type: 'LessThan',
    object: 10
};

// you can also define an equality predicate with the `equals` helper method
import {lessThan} from '@aws/dynamodb-expressions';

equalsExpressionPredicate = lessThan(10);

// combine with a subject to create a valid condition expression
const equalsExpression: ConditionExpression = {
    ...equalsExpressionPredicate,
    subject: 'foo'
};
```

`object` may be an [attribute path](#attribute-paths), an [attribute
value](#attribute-values), or another type. If the lattermost type is received,
it will be serialized using the `@aws/dynamodb-auto-marshaller` package.

#### `LessThanOrEqualTo` expression predicate

Creates a condition which is true if the defined `subject` is less than or equal
to the defined `object`. For example, the following predicate object asserts
that the subject is less than or equal to 10:

```typescript
import {
    ConditionExpression,
    ConditionExpressionPredicate,
} from '@aws/dynamodb-expressions';

let equalsExpressionPredicate: ConditionExpressionPredicate = {
    type: 'LessThanOrEqualTo',
    object: 10
};

// you can also define an equality predicate with the `equals` helper method
import {lessThanOrEqualTo} from '@aws/dynamodb-expressions';

equalsExpressionPredicate = lessThanOrEqualTo(10);

// combine with a subject to create a valid condition expression
const equalsExpression: ConditionExpression = {
    ...equalsExpressionPredicate,
    subject: 'foo'
};
```

`object` may be an [attribute path](#attribute-paths), an [attribute
value](#attribute-values), or another type. If the lattermost type is received,
it will be serialized using the `@aws/dynamodb-auto-marshaller` package.

#### `GreaterThan` expression predicate

Creates a condition which is true if the defined `subject` is greater than the
defined `object`. For example, the following predicate object asserts that the
subject is greater than 10:

```typescript
import {
    ConditionExpression,
    ConditionExpressionPredicate,
} from '@aws/dynamodb-expressions';

let equalsExpressionPredicate: ConditionExpressionPredicate = {
    type: 'GreaterThan',
    object: 10
};

// you can also define an equality predicate with the `equals` helper method
import {greaterThan} from '@aws/dynamodb-expressions';

equalsExpressionPredicate = greaterThan(10);

// combine with a subject to create a valid condition expression
const equalsExpression: ConditionExpression = {
    ...equalsExpressionPredicate,
    subject: 'foo'
};
```

`object` may be an [attribute path](#attribute-paths), an [attribute
value](#attribute-values), or another type. If the lattermost type is received,
it will be serialized using the `@aws/dynamodb-auto-marshaller` package.

#### `GreaterThanOrEqualTo` expression predicate

Creates a condition which is true if the defined `subject` is greater than or
equal to the defined `object`. For example, the following predicate object
asserts that the subject is greater than or equal to 10:

```typescript
import {
    ConditionExpression,
    ConditionExpressionPredicate,
} from '@aws/dynamodb-expressions';

let equalsExpressionPredicate: ConditionExpressionPredicate = {
    type: 'GreaterThanOrEqualTo',
    object: 10
};

// you can also define an equality predicate with the `equals` helper method
import {greaterThanOrEqualTo} from '@aws/dynamodb-expressions';

equalsExpressionPredicate = greaterThanOrEqualTo(10);

// combine with a subject to create a valid condition expression
const equalsExpression: ConditionExpression = {
    ...equalsExpressionPredicate,
    subject: 'foo'
};
```

`object` may be an [attribute path](#attribute-paths), an [attribute
value](#attribute-values), or another type. If the lattermost type is received,
it will be serialized using the `@aws/dynamodb-auto-marshaller` package.

#### `Between` expression predicate

Creates a condition which is true if the defined `subject` is between a defined
`lowerBound` and `upperBound`. For example, the following predicate object
asserts that the subject is greater than or equal to 10 and less than or equal
to 99:

```typescript
import {
    ConditionExpression,
    ConditionExpressionPredicate,
} from '@aws/dynamodb-expressions';

let equalsExpressionPredicate: ConditionExpressionPredicate = {
    type: 'Between',
    lowerBound: 10,
    upperBound: 99
};

// you can also define an equality predicate with the `equals` helper method
import {between} from '@aws/dynamodb-expressions';

equalsExpressionPredicate = between(10, 99);

// combine with a subject to create a valid condition expression
const equalsExpression: ConditionExpression = {
    ...equalsExpressionPredicate,
    subject: 'foo'
};
```

`lowerBound` and `upperBound` may both be an [attribute path](#attribute-paths),
an [attribute value](#attribute-values), or another type. If the lattermost type
is received, it will be serialized using the `@aws/dynamodb-auto-marshaller`
package.

#### `Membership` expression predicate

Creates a condition which is true if the defined `subject` is equal to a member
of a list of defined values. For example, the following predicate object asserts
that the subject is one of `'fizz'`, `'buzz'`, or `'fizzbuzz'`:

```typescript
import {
    ConditionExpression,
    ConditionExpressionPredicate,
} from '@aws/dynamodb-expressions';

let equalsExpressionPredicate: ConditionExpressionPredicate = {
    type: 'Membership',
    values: ['fizz', 'buzz', 'fizzbuzz']
};

// you can also define an equality predicate with the `equals` helper method
import {inList} from '@aws/dynamodb-expressions';

equalsExpressionPredicate = inList('fizz', 'buzz', 'fizzbuzz');

// combine with a subject to create a valid condition expression
const equalsExpression: ConditionExpression = {
    ...equalsExpressionPredicate,
    subject: 'foo'
};
```

Each value in the `values` array may be an [attribute path](#attribute-paths),
an [attribute value](#attribute-values), or another type. If the lattermost type
is received, it will be serialized using the `@aws/dynamodb-auto-marshaller`
package.

### Serializing condition expressions

To serialize a condition expression, pass a `ConditionExpression` object and an
instance of `ExpressionAttributes`.

## Expression attributes

Amazon DynamoDB expressions are serialized as strings with semantically
important control characters and reserved words. The `ExpressionAttributes`
object will escape both attribute names and attribute values for safe use in
any expression. When a full DynamoDB request input is ready to be sent, you can
retrieve a the `ExpressionAttributeNames` and `ExpressionAttributeValues` shapes
to send alongside the input:

```typescript
import {
    AttributePath,
    AttributeValue,
    ExpressionAttributes,
} from '@aws/dynamodb-expressions';
const DynamoDb = require('aws-sdk/clients/dynamodb');

const attributes = new ExpressionAttributes();

// you can add a string attribute name
const escapedFoo = attributes.addName('foo');
// or a complex path
const escapedPath = attributes.addName('bar.baz[3].snap.crackle.pop');
// or an already parsed attribute path
attributes.addName(new AttributePath('path.to.nested.field'));

// raw JavaScript values added will be converted to AttributeValue objects
const escapedRaw = attributes.addValue(42);
// already marshalled values must be wrapped in an AttributeValue object
const escapedMarshalled = attributes.addValue(new AttributeValue({N: "42"}));

const client = new DynamoDb();
client.query({
    TableName: 'my_table',
    KeyConditionExpression: `${escapedFoo} = ${escapedRaw} AND ${escapedPath} = ${escapedMarshalled}`,
    ExpressionAttributeNames: attributes.names,
    ExpressionAttributeValues: attributes.values,
})
```

## Function expressions

Function expressions represent named functions that DynamoDB will execute on
your behalf. The first parameter passed to the `FunctionExpression` represents
the function name and must be a string; all subsequent parameters represent
arguments to pass to the function. These parameters may be instances of
`AttributePath` (to have the function evaluate part of the DynamoDB document to
which the function applies), `AttributeValue` (for already-marshalled
AttributeValue objects), or arbitrary JavaScript values (these will be converted
by the `@aws/dynamodb-auto-marshaller` package's `Marshaller`):

```typescript
import {
    AttributePath,
    ExpressionAttributes,
    FunctionExpression,
} from '@aws/dynamodb-expressions';

const expr = new FunctionExpression(
    'list_append', 
    new AttributePath('path.to.list'),
    'foo'
);
const attributes = new ExpressionAttributes();
// serializes as 'list_append(#attr0.#attr1.#attr2, :val3)'
const serialized = expr.serialize(attributes);
console.log(attributes.names); // {'#attr0': 'path', '#attr1': 'to', '#attr2': 'list'}
console.log(attributes.values); // {':val3': {S: 'foo'}}
```

## Mathematical expressions

Mathematical expressions are used in the `SET` clause of update expressions to
add or subtract numbers from attribute properties containing number values:

```typescript
import {MathematicalExpression} from '@aws/dynamodb-expressions';

const expr = new MathematicalExpression('version', '+', 1);
```

## Projection Expressions

Projection expressions tell DynamoDB which attributes to include in fetched
records returned by `GetItem`, `Query`, or `Scan` operations. This library uses
`ProjectionExpression` as a type alias for an array of strings and
`AttributePath` objects.

## Update Expressions

Update expressions allow the partial, in place update of a record in DynamoDB.
The expression may have up to four clauses, one containing directives to set
values in the record, one containing directives to remove attributes from the
record, one containing directives to add values to a set, and the last
containing directives to delete values from a set.

```typescript
import {
    AttributePath,
    FunctionExpression,
    UpdateExpression,
} from '@aws/dynamodb-expressions';

const expr = new UpdateExpression();

// set a value by providing its key and the desired value
expr.set('foo', 'bar');
// you may also set properties in nested maps and lists
expr.set(
    'path.to.my.desired[2].property',
    new FunctionExpression(
        'list_append',
        new AttributePath('path.to.my.desired[2].property'),
        'baz'   
    )
);

// remove a value by providing its key or path
expr.remove('fizz.buzz.pop[0]');

// add a value to a set
expr.add('string_set', 'foo');

// delete a value from the same set
expr.delete('string_set', 'bar');
```
