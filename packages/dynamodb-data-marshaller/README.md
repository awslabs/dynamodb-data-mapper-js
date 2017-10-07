# Amazon DynamoDB Data Marshaller

[![Apache 2 License](https://img.shields.io/github/license/awslabs/dynamodb-data-mapper-js.svg?style=flat)](http://aws.amazon.com/apache-2-0/)

This library provides an `marshallItem` and `unmarshallItem` functions that
convert native JavaScript values to DynamoDB AttributeValues and back again,
respectively, based on a defined schema. While many JavaScript values map
cleanly to DynamoDB data types and vice versa, schemas allow you to losslessly
persist any JavaScript type, including dates, class instances, and empty
strings.

## Getting started

To use the data marshaller, begin by defining a schema that describes the
relationship between your application's domain objects and their serialized form
in a DynamoDB table:

```javascript
const schema = {
    foo: {type: 'Binary'},
    bar: {type: 'Boolean'},
    baz: {type: 'String'},
    quux: {
        type: 'Document',
        members: {
            fizz: {type: 'Set', memberType: 'String'},
            buzz: {
                type: 'Tuple',
                members: [
                    {
                        type: 'List',
                        memberType: {type: 'Set', memberType: 'Number'},
                    },
                    {
                        type: 'Map',
                        memberType: {type: 'Date'},
                    }
                ]
            },
        },
    },
};
```

This schema may be used to marshall JavaScript values to DynamoDB attribute
values:

```javascript
import {marshallItem} from '@aws/dynamodb-data-marshaller';

const marshalled = marshallItem(schema, {
    foo: Uint8Array.from([0xde, 0xad, 0xbe, 0xef]),
    bar: false,
    baz: '',
    quux: {
        fizz: new Set(['a', 'b', 'c']),
        buzz: [
            [
                new Set([1, 2, 3]),
                new Set([2, 3, 4]),
                new Set([3, 4, 5]),
            ],
            new Map([
                ['now', new Date()],
                ['then', new Date(0)],
            ]),
        ]
    }
});
```

The schema can also be used to unmarshall DynamoDB attribute values back to
their original JavaScript representation:

```javascript
import {unmarshallItem} from '@aws/dynamodb-data-marshaller';

const unmarshalled = unmarshallItem(schema, {
    foo: {B: Uint8Array.from([0xde, 0xad, 0xbe, 0xef])},
    bar: {BOOL: false},
    baz: {NULL: true},
    quux: {
        fizz: {SS: ['a', 'b', 'c']},
        buzz: {
            L: [
                L: [
                    {NS: ['1', '2', '3']},
                    {NS: ['2', '3', '4']},
                    {NS: ['3', '4', '5']},
                ],
                M: {
                    now: {N: '1507189047'},
                    then: {N: '0'}
                },
            ],
        },
    },
});
```

## Specifying keys

DynamoDB tables must define a hash key and may optionally define a range key. In
DynamoDB documentation, these keys are sometimes referred to as *partition* and
*sort* keys, respectively. To declare a property to be a key, add a `keyType`
property to its property schema (example taken from the [DynamoDB developer
guide](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html)):

```javascript
// Table model taken from http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
const gameScores = {
    UserId: {
        type: 'String',
        keyType: 'HASH'
    },
    GameTitle: {
        type: 'String',
        keyType: 'RANGE'
    },
    TopScore: {type: 'Number'},
    TopScoreDateTime: {type: 'Date'},
    Wins: {type: 'Number'},
    Losses: {type: 'Number'}
};
```

The `keyType` attribute may only be used in types that are serialized as
strings, numbers, or binary attributes. In addition to `'String'`, `'Number'`,
and `'Binary'` properties, it may be used on `'Date'` and `'Custom'` properties.

Index keys are specified using an object mapping index names to the key type as
which the value is used in a given index. To continue with the `gameScores`
example given above, you could add the index key declarations described in [the 
DynamoDB Global Secondary Index developer guide](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html)
as follows:

```javascript
const gameScores = {
    UserId: {
        type: 'String',
        keyType: 'HASH'
    },
    GameTitle: {
        type: 'String',
        keyType: 'RANGE',
        indexKeyConfigurations: {
            GameTitleIndex: 'HASH'
        }
    },
    TopScore: {
        type: 'Number',
        indexKeyConfigurations: {
            GameTitleIndex: 'RANGE'
        }
    },
    TopScoreDateTime: {type: 'Date'},
    Wins: {type: 'Number'},
    Losses: {type: 'Number'}
};
```

## Supplying defaults

Any property schema may define a `defaultProvider` function to be called when a
field is `undefined` in the input provided to `marshallItem`. This function must
return a raw JavaScript value and should not return an already-marshalled
DynamoDB AttributeValue shape.

```javascript
const uuidV4 = require('uuid/v4');

const schema = {
    key: {
        type: 'String',
        defaultProvider: uuidV4,
        keyType: 'HASH',
    },
    // ...
};
```

## Supported types

### Any

Will be marshalled and unmarshalled using the `@aws/dynamodb-auto-marshaller`
package, which detects the type of a given value at runtime.

#### Example

```javascript
const anyProperty = {
    type: 'Any',
    // optionally, you may specify configuration options for the
    // @aws/dynamodb-auto-marshaller package's Marshaller class:
    unwrapNumbers: false,
    onInvalid: 'omit',
    onEmpty: 'nullify',
};
```

### Binary

Used for `ArrayBuffer` and `ArrayBufferView` objects, as well as Node.JS
buffers.

**May be used as a table or index key.**

#### Example

```javascript
const binaryProperty = {type: 'Binary'};
```

### Boolean

Used for `true`/`false` values.

#### Example

```javascript
const booleanProperty = {type: 'Boolean'};
```

### Collection

Denotes a list of untyped items. The constituent items will be marshalled and
unmarshalled using the `@aws/dynamodb-auto-marshaller`.

#### Example

```javascript
const collectionProperty = {
    type: 'Collection',
    // optionally, you may specify configuration options for the
    // @aws/dynamodb-auto-marshaller package's Marshaller class:
    unwrapNumbers: false,
    onInvalid: 'omit',
    onEmpty: 'nullify',
};
```

### Custom

Allows the use of bespoke marshalling and unmarshalling functions. The type
definition for a `'Custom'` property must include a `marshall` function that
converts the type's JavaScript representation to a DynamoDB AttributeValue and
an `unmarshall` function that converts the AttributeValue back to a JavaScript
value.

**May be used as a table or index key.**

#### Example

```javascript
// This custom property handles strings
const customProperty = {
    type: 'Custom',
    marshall(input) {
        return {S: input};
    },
    unmarshall(persistedValue) {
        return persistedValue.S;
    }
};
```

### Date

Used for time data. Dates will be serialized to DynamoDB as epoch timestamps
for easy integration with DynamoDB's time-to-live feature. As a result, timezone
information will not be persisted.

**May be used as a table or index key.**

#### Example

```javascript
const dateProperty = {type: 'Date'};
```

### Document

Used for object values that have their own schema and (optionally) constructor.

#### Example

```javascript
class MyCustomDocument {
    method() {
        // pass
    }
    
    get computedProperty() {
        // pass
    }
}

class documentSchema = {
    fizz: {type: 'String'},
    buzz: {type: 'Number'},
    pop: {type: 'Date'}
}

const documentProperty = {
    type: 'Document',
    members: documentSchema,
    // optionally, you may specify a constructor to use to create the object
    // that will underlie unmarshalled instances. If not specified,
    // Object.create(null) will be used.
    valueConstructor: MyCustomDocument
};
```

### Hash

Used for objects with string keys and untyped values.

#### Example

```javascript
const collectionProperty = {
    type: 'Hash',
    // optionally, you may specify configuration options for the
    // @aws/dynamodb-auto-marshaller package's Marshaller class:
    unwrapNumbers: false,
    onInvalid: 'omit',
    onEmpty: 'nullify',
};
```

### List

Used for arrays or iterable objects whose elements are all of the same type.

#### Example

```javascript
const listOfStrings = {
    type: 'List',
    memberType: {type: 'String'}
};
```

### Map

Used for `Map` objects whose values are all of the same type.

#### Example

```javascript
const mapOfStrings = {
    type: 'Map',
    memberType: {type: 'String'}
};
```

### Null

Used to serialize `null`. Often used as a sigil value.

#### Example

```javascript
const nullProperty = {type: 'Null'};
```

### Number

Used to serialize numbers.

**May be used as a table or index key.**

#### Example

```javascript
const numberProperty = {type: 'Number'};
```

### Set

Used to serialize sets whose values are all of the same type. DynamoDB allows
sets of numbers, sets of strings, and sets of binary values.

#### Example

```javascript
const binarySetProperty = {type: 'Set', memberType: 'Binary'};
const numberSetProperty = {type: 'Set', memberType: 'Number'};
const stringSetProperty = {type: 'Set', memberType: 'String'};
```

### String

Used to serialize strings.

**May be used as a table or index key.**

#### Example

```javascript
const stringProperty = {type: 'String'};
```

### Tuple

Used to store arrays that have a specific length and sequence of elements.

#### Example

```javascript
const tupleProperty = {
    type: 'Tuple',
    members: [
        {type: 'Boolean'},
        {type: 'String'}
    ]
};
```
