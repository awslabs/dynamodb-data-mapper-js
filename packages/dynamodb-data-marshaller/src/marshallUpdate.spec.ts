import {marshallUpdate} from './marshallUpdate';
import {Schema} from "./Schema";
import {TableDefinition} from "./TableDefinition";
import {OnMissingStrategy} from "./OnMissingStrategy";

const tableDef: TableDefinition = {
    tableName: 'table',
    schema: {
        foo: {
            type: 'String',
            keyType: 'HASH',
            attributeName: 'fizz'
        },
        bar: {
            type: 'Tuple',
            members: [
                {type: 'Number'},
                {type: 'Binary'},
            ],
            attributeName: 'buzz',
        },
        quux: {
            type: 'Document',
            members: {
                snap: { type: 'String' },
                crackle: { type: 'Date' },
                pop: { type: 'Hash' },
            } as Schema,
        },
    },
};

describe('marshallUpdate', () => {
    it('should marshall updates into an UpdateItemInput', () => {
        const marshalled = marshallUpdate({
            tableDefinition: tableDef,
            input: {
                foo: 'key',
                bar: [1, Uint8Array.from([0xde, 0xad, 0xbe, 0xef])]
            },
        });

        expect(marshalled).toEqual({
            TableName: 'table',
            Key: {
                fizz: {S: 'key'}
            },
            ExpressionAttributeNames: {
                '#attr0': 'buzz',
                '#attr2': 'quux',
            },
            ExpressionAttributeValues: {
                ':val1': {
                    L: [
                        {N: '1'},
                        {B: Uint8Array.from([0xde, 0xad, 0xbe, 0xef])}
                    ],
                }
            },
            UpdateExpression: 'SET #attr0 = :val1 REMOVE #attr2',
        });
    });

    it('should not remove missing keys when onMissing is "SKIP"', () => {
        const marshalled = marshallUpdate({
            tableDefinition: tableDef,
            input: {
                foo: 'key',
                bar: [1, Uint8Array.from([0xde, 0xad, 0xbe, 0xef])]
            },
            onMissing: OnMissingStrategy.Skip
        });

        expect(marshalled).toEqual({
            TableName: 'table',
            Key: {
                fizz: {S: 'key'}
            },
            ExpressionAttributeNames: {
                '#attr0': 'buzz',
            },
            ExpressionAttributeValues: {
                ':val1': {
                    L: [
                        {N: '1'},
                        {B: Uint8Array.from([0xde, 0xad, 0xbe, 0xef])}
                    ],
                }
            },
            UpdateExpression: 'SET #attr0 = :val1',
        });
    });

    describe('version attributes', () => {
        const tableDef: TableDefinition = {
            tableName: 'table',
            schema: {
                foo: {
                    type: 'String',
                    keyType: 'HASH',
                    attributeName: 'fizz'
                },
                bar: {
                    type: 'Tuple',
                    members: [
                        {type: 'Number'},
                        {type: 'Binary'},
                    ],
                    attributeName: 'buzz',
                },
                baz: {
                    type: 'Number',
                    versionAttribute: true,
                },
            },
        };

        it(
            'should inject a conditional expression requiring the absence of the versioning property and set its value to 0 when an object without a value for it is marshalled',
            () => {

                const marshalled = marshallUpdate({
                    tableDefinition: tableDef,
                    input: {
                        foo: 'key',
                        bar: [1, Uint8Array.from([0xde, 0xad, 0xbe, 0xef])]
                    },
                });

                expect(marshalled).toEqual({
                    TableName: 'table',
                    Key: {
                        fizz: {S: 'key'}
                    },
                    ConditionExpression: 'attribute_not_exists(#attr2)',
                    ExpressionAttributeNames: {
                        '#attr0': 'buzz',
                        '#attr2': 'baz',
                    },
                    ExpressionAttributeValues: {
                        ':val1': {
                            L: [
                                {N: '1'},
                                {B: Uint8Array.from([0xde, 0xad, 0xbe, 0xef])}
                            ],
                        },
                        ':val3': {N: '0'},
                    },
                    UpdateExpression: 'SET #attr0 = :val1, #attr2 = :val3',
                });
            }
        );

        it(
            'should inject a conditional expression requiring the known value of the versioning property and set its value to the previous value + 1 when an object with a value for it is marshalled',
            () => {

                const marshalled = marshallUpdate({
                    tableDefinition: tableDef,
                    input: {
                        foo: 'key',
                        bar: [1, Uint8Array.from([0xde, 0xad, 0xbe, 0xef])],
                        baz: 10,
                    },
                });

                expect(marshalled).toEqual({
                    TableName: 'table',
                    Key: {
                        fizz: {S: 'key'}
                    },
                    ConditionExpression: '#attr2 = :val3',
                    ExpressionAttributeNames: {
                        '#attr0': 'buzz',
                        '#attr2': 'baz',
                    },
                    ExpressionAttributeValues: {
                        ':val1': {
                            L: [
                                {N: '1'},
                                {B: Uint8Array.from([0xde, 0xad, 0xbe, 0xef])}
                            ],
                        },
                        ':val3': {N: '10'},
                        ':val4': {N: '1'},
                    },
                    UpdateExpression: 'SET #attr0 = :val1, #attr2 = #attr2 + :val4',
                });
            }
        );
    });
});
