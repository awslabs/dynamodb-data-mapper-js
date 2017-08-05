import {unmarshallItem} from "../lib/unmarshallItem";
import {Schema} from "../lib/Schema";
import {BinarySet} from "@aws/dynamodb-auto-marshaller";
import * as AWS from 'aws-sdk';

describe('unmarshallItem', () => {
    it('should unmarshall fields from their attributeName if provided', () => {
        const attributeName = 'binVal';
        const schema: Schema = {
            binary: {type: 'Binary', attributeName},
        };

        expect(unmarshallItem(
            schema,
            {[attributeName]: {B: new Uint8Array(15)}}
        )).toEqual({binary: new Uint8Array(15)});
    });

    it('should ignore fields not mentioned in the schema', () => {
        const schema: Schema = {
            binary: {type: 'Binary'},
        };

        expect(unmarshallItem(schema, {str: {S: 'a string'}})).toEqual({});
    });

    it('should ignore fields whose type differs from that in the schema', () => {
        const schema: Schema = {
            binary: {type: 'Binary'},
        };

        expect(unmarshallItem(schema, {binary: {S: 'a string'}})).toEqual({});
    });

    it('should throw if the schema type tag is not recognized', () => {
        const schema: Schema = {
            binary: {type: 'Foo'} as any,
        };

        expect(() => unmarshallItem(schema, {binary: {S: 'a string'}}))
            .toThrow();
    });

    describe('binary fields', () => {
        const schema: Schema = {
            binary: {type: 'Binary'},
        };

        it('should unmarshall binary fields', () => {
            expect(unmarshallItem(schema, {binary: {B: new Uint8Array(15)}}))
                .toEqual({binary: new Uint8Array(15)});
        });

        it('should convert null values to an empty binary value', () => {
            expect(unmarshallItem(schema, {binary: {NULL: true}}))
                .toEqual({binary: new Uint8Array(0)});
        });
    });

    describe('binary set fields', () => {
        const schema: Schema = {
            binSet: {type: 'BinarySet'},
        };

        it('should unmarshall binary set fields', () => {
            const attrMap = {
                binSet: {
                    BS: [
                        new Uint8Array(1),
                        new Uint8Array(2),
                        new Uint8Array(3),
                    ],
                },
            };

            expect(unmarshallItem(schema, attrMap)).toEqual({
                binSet: new BinarySet(attrMap.binSet.BS),
            });
        });

        it('should unmarshall null values as empty binary sets', () => {
            expect(unmarshallItem(schema, {binSet: {NULL: true}}))
                .toEqual({binSet: new BinarySet()});
        });

        it('should unmarshall type mismatches as undefined', () => {
            expect(unmarshallItem(schema, {binSet: {BOOL: true}}))
                .toEqual({binSet: void 0});
        });
    });

    describe('boolean fields', () => {
        const schema: Schema = {
            boolean: {type: 'Boolean'},
        };

        it('should unmarshall boolean fields', () => {
            expect(unmarshallItem(schema, {boolean: {BOOL: false}}))
                .toEqual({boolean: false});
        });
    });

    describe('collection fields', () => {
        it('should unmarshall untyped collections', () => {
            const schema: Schema = {mixedList: {type: 'Collection'}};
            const input = {
                mixedList: {
                    L: [
                        {S: 'string'},
                        {N: '123'},
                        {B: new Uint8Array(12)},
                        {M: {foo: {S: 'bar'}}},
                        {L: [
                            {S: 'one string'},
                            {N: '234'},
                            {B: new Uint8Array(5)},
                        ]},
                    ],
                },
            };

            expect(unmarshallItem(schema, input)).toEqual({
                mixedList: [
                    'string',
                    123,
                    new (AWS as any).util.Buffer(12),
                    {foo: 'bar'},
                    ['one string', 234, new (AWS as any).util.Buffer(5)],
                ]
            });
        });
    });

    describe('custom fields', () => {
        it(
            'should unmarshall custom fields by invoking the unmarshaller defined in the schema',
            () => {
                const unmarshall = jest.fn(() => 'unmarshalled');
                const schema: Schema = {
                    custom: {
                        type: 'Custom',
                        marshall: jest.fn(),
                        unmarshall,
                    },
                };

                expect(unmarshallItem(schema, {custom: {NULL: true}}))
                    .toEqual({custom: 'unmarshalled'});

                expect(unmarshall.mock.calls.length).toBe(1);
                expect(unmarshall.mock.calls[0]).toEqual([{NULL: true}]);
            }
        );
    });

    describe('date fields', () => {
        const schema: Schema = {aDate: {type: 'Date'}};
        const iso8601 = '2000-01-01T00:00:00Z';
        const epoch = 946684800;

        it('should unmarshall dates into Date objects', () => {
            expect(unmarshallItem(schema, {aDate: {N: epoch.toString(10)}}))
                .toEqual({aDate: new Date(iso8601)});
        });

        it(
            'should leaves dates undefined if the value at the designated key is not a number',
            () => {
                expect(unmarshallItem(schema, {aDate: {S: epoch.toString(10)}}))
                    .toEqual({});
            }
        );
    });

    describe('document fields', () => {
        it('should recursively unmarshall documents', () => {
            const schema: Schema = {
                nested: {
                    type: 'Document',
                    members: {
                        nested: {
                            type: 'Document',
                            members: {
                                scalar: {type: 'String'},
                            },
                        },
                    },
                },
            };
            const input = {
                nested: {
                    M: {
                        nested: {
                            M: {
                                scalar: {
                                    S: 'value',
                                },
                            },
                        },
                    },
                },
            };

            expect(unmarshallItem(schema, input))
                .toEqual({nested: {nested: {scalar: 'value'}}});
        });

        it(
            'should invoke the constructor defined in the schema for documents',
            () => {
                const ctor = class {};
                const schema: Schema = {
                    ctorDoc: {
                        type: 'Document',
                        members: {},
                        valueConstructor: ctor,
                    }
                };

                const unmarshalled = unmarshallItem(
                    schema,
                    {ctorDoc: {M: {}}},
                );
                expect(unmarshalled.ctorDoc).toBeInstanceOf(ctor);
            }
        );

        it('should return undefined for unexpected types', () => {
            const schema: Schema = {
                doc: {
                    type: 'Document',
                    members: {},
                }
            };

            expect(unmarshallItem(schema, {doc: {L: []}})).toEqual({});
        });
    });

    describe('hash fields', () => {
        it('should unmarshall untyped hashes', () => {
            const schema: Schema = {mixedHash: {type: 'Hash'}};
            const input = {
                mixedHash: {
                    M: {
                        foo: {S: 'string'},
                        bar: {N: '123'},
                        baz: {B: new Uint8Array(12)},
                        fizz: {M: {foo: {S: 'bar'}}},
                        buzz: {
                            L: [
                                {S: 'one string'},
                                {N: '234'},
                                {B: new Uint8Array(5)},
                            ]
                        },
                    },
                },
            };

            expect(unmarshallItem(schema, input)).toEqual({
                mixedHash: {
                    foo: 'string',
                    bar: 123,
                    baz: new (AWS as any).util.Buffer(12),
                    fizz: {foo: 'bar'},
                    buzz: ['one string', 234, new (AWS as any).util.Buffer(5)],
                }
            });
        });
    });

    describe('list fields', () => {
        const schema: Schema = {
            list: {
                type: 'List',
                memberType: {type: 'String'},
            },
        };

        it('should unmarshall lists of like items', () => {
            expect(unmarshallItem(
                schema,
                {
                    list: {
                        L: [
                            {S: 'a'},
                            {S: 'b'},
                            {S: 'c'},
                        ],
                    },
                }
            )).toEqual({list: ['a', 'b', 'c']});
        });

        it('should unmarshall non-lists as undefined', () => {
            expect(unmarshallItem(schema, {list: {S: 's'}})).toEqual({});
        });
    });

    describe('map fields', () => {
        const schema: Schema = {
            map: {
                type: 'Map',
                memberType: {type: 'String'},
            },
        };

        it('should unmarshall maps of string keys to like items', () => {
            expect(unmarshallItem(
                schema,
                {
                    map: {
                        M: {
                            foo: {S: 'bar'},
                            fizz: {S: 'buzz'},
                        },
                    },
                }
            ))
                .toEqual({
                    map: new Map<string, string>([
                        ['foo', 'bar'],
                        ['fizz', 'buzz'],
                    ])
                });
        });

        it('should unmarshall unexpected types as undefined', () => {
            expect(unmarshallItem(schema, {map: {S: 'foo'}})).toEqual({});
        });
    });

    describe('null fields', () => {
        const schema: Schema = {
            'null': {type: 'Null'},
        };

        it('should unmarshall null fields', () => {
            expect(unmarshallItem(schema, {'null': {NULL: true}}))
                .toEqual({'null': null});
        });

        it('should unmarshall unexpected types as undefined', () => {
            expect(unmarshallItem(schema, {'null': {S: 'b'}})).toEqual({});
        });
    });

    describe('number fields', () => {
        const schema: Schema = {
            number: {type: 'Number'},
        };

        it('should unmarshall number fields', () => {
            expect(unmarshallItem(schema, {number: {N: '123'}}))
                .toEqual({number: 123});
        });

        it('should unmarshall unexpected types as undefined', () => {
            expect(unmarshallItem(schema, {number: {S: '123'}})).toEqual({});
        });
    });

    describe('number set fields', () => {
        const schema: Schema = {
            numSet: { type: 'NumberSet'},
        };

        it('should unmarshall number set fields', () => {
            expect(unmarshallItem(
                schema,
                {numSet: {NS: ['1', '2', '3']}}
            )).toEqual({numSet: new Set([1, 2, 3])});
        });

        it('should unmarshall null values as empty sets', () => {
            expect(unmarshallItem(schema, {numSet: {NULL: true}}))
                .toEqual({numSet: new Set()});
        });

        it('should unmarshall unexpected types as undefined', () => {
            expect(unmarshallItem(
                schema,
                {numSet: {SS: ['1', '2', '3']}}
            )).toEqual({});
        });
    });

    describe('string fields', () => {
        const schema: Schema = {
            string: {type: 'String'},
        };

        it('should unmarshall string fields', () => {
            expect(unmarshallItem(schema, {string: {S: 'string'}}))
                .toEqual({string: 'string'});
        });

        it('should unmarshall null values as empty strings', () => {
            expect(unmarshallItem(schema, {string: {NULL: true}}))
                .toEqual({string: ''});
        });
    });

    describe('string set fields', () => {
        const schema: Schema = {
            strSet: { type: 'StringSet'},
        };

        it('should unmarshall string set fields', () => {
            expect(unmarshallItem(
                schema,
                {strSet: {SS: ['a', 'b', 'c']}}
            )).toEqual({strSet: new Set(['a', 'b', 'c'])});
        });

        it('should unmarshall null values as empty sets', () => {
            expect(unmarshallItem(schema, {strSet: {NULL: true}}))
                .toEqual({strSet: new Set()});
        });

        it('should unmarshall unexpected types as undefined', () => {
            expect(unmarshallItem(
                schema,
                {strSet: {NS: ['a', 'b', 'c']}}
            )).toEqual({});
        });
    });

    describe('tuple fields', () => {
        const schema: Schema = {
            jobResult: {
                type: 'Tuple',
                members: [
                    {type: 'Boolean'},
                    {type: 'Number'},
                ],
            }
        };

        it('should unmarshall tuples', () => {
            expect(unmarshallItem(
                schema,
                {
                    jobResult: {
                        L: [
                            {BOOL: true},
                            {N: '123'},
                        ],
                    },
                }
            )).toEqual({jobResult: [true, 123]});
        });

        it('should unmarshall unexpected types as undefined', () => {
            expect(unmarshallItem(schema, {jobResult: {BOOL: true}}))
                .toEqual({});
        });
    });
});
