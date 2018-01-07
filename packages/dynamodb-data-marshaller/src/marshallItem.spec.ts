import {marshallItem} from "./marshallItem";
import {Schema} from "./Schema";
import {CustomType} from "./SchemaType";
import objectContaining = jasmine.objectContaining;
import {BinarySet} from "@aws/dynamodb-auto-marshaller";

describe('marshallItem', () => {
    it('should serialize fields to their attributeName if provided', () => {
        const schema: Schema = {
            boolean: {
                type: 'Boolean',
                attributeName: 'bool_field',
            },
        };
        expect(marshallItem(schema, {boolean: true})).toEqual({
            bool_field: {BOOL: true},
        });
    });

    it('should ignore fields not mentioned in the schema', () => {
        expect(marshallItem({foo: {type: 'String'}}, {bar: 'baz'})).toEqual({});
    });

    it('should ignore fields whose value is undefined', () => {
        expect(marshallItem({foo: {type: 'String'}}, {foo: void 0}))
            .toEqual({});
    });

    it('should throw if the schema type tag is not recognized', () => {
        expect(() => marshallItem({foo: {type: 'Foo'}} as any, {foo: 'bar'}))
            .toThrow('Unrecognized schema node');
    });

    describe('default values', () => {
        it(
            'should call a defined default provider if the input is undefined',
            () => {
                const defaultProvider = jest.fn(() => 'foo');
                expect(marshallItem(
                    {foo: {type: 'String', defaultProvider}},
                    {foo: void 0}
                )).toEqual({foo: {S: 'foo'}});

                expect(defaultProvider.mock.calls.length).toBe(1);
            }
        );

        it('should not call the default provider if the input is defined', () => {
            const defaultProvider = jest.fn(() => 'foo');
            expect(marshallItem(
                {foo: {type: 'String', defaultProvider}},
                {foo: 'bar'}
            )).toEqual({foo: {S: 'bar'}});

            expect(defaultProvider.mock.calls.length).toBe(0);
        });
    });

    describe('"any" (untyped) fields', () => {
        it('should marshall of untyped data', () => {
            const schema: Schema = {mixedList: {type: 'Any'}};
            const input = {
                mixedList: [
                    'string',
                    123,
                    undefined,
                    new ArrayBuffer(12),
                    {foo: 'bar'},
                    ['one string', 234, new ArrayBuffer(5)],
                ]
            };

            expect(marshallItem(schema, input)).toEqual({
                mixedList: {
                    L: [
                        {S: 'string'},
                        {N: '123'},
                        {B: new ArrayBuffer(12)},
                        {M: {foo: {S: 'bar'}}},
                        {L: [
                            {S: 'one string'},
                            {N: '234'},
                            {B: new ArrayBuffer(5)},
                        ]},
                    ],
                },
            });
        });
    });

    describe('binary fields', () => {
        it('should serialize fields of binary types from ArrayBuffers', () => {
            const binaryDoc: Schema = {
                binary: {type: 'Binary'},
            };
            const document = {
                binary: new ArrayBuffer(15),
            };

            expect(marshallItem(binaryDoc, document)).toEqual({
                binary: {B: new Uint8Array(15)},
            });
        });

        it('should serialize binary fields from ArrayBufferViews', () => {
            const binaryDoc: Schema = {
                binary: {type: 'Binary'},
            };
            const document = {
                binary: new Int32Array(4),
            };

            expect(marshallItem(binaryDoc, document)).toEqual({
                binary: {B: new Uint8Array(16)},
            });
        });

        it('should convert UTF-8 strings to Uint8Arrays', () => {
            const binaryDoc: Schema = {
                binary: {type: 'Binary'},
            };
            const document = {
                binary: '☃💩',
            };

            expect(marshallItem(binaryDoc, document)).toEqual({
                binary: {B: new Uint8Array([226, 152, 131, 240, 159, 146, 169])},
            });
        });

        it('should convert empty binary values to NULL', () => {
            const binaryDoc: Schema = {
                binary: {type: 'Binary'},
            };
            const document = {
                binary: new Int32Array(0),
            };

            expect(marshallItem(binaryDoc, document)).toEqual({
                binary: {NULL: true},
            });
        });
    });

    describe('binary set fields', () => {
        const schema: Schema = {
            binSet: { type: 'Set', memberType: 'Binary'},
        };

        it('should serialize BinarySet fields', () => {
            expect(marshallItem(
                schema,
                {
                    binSet: new BinarySet([
                        new Uint8Array(1),
                        new Uint8Array(2),
                        new Uint8Array(3),
                    ])
                }
            )).toEqual({
                binSet: {
                    BS: [
                        new Uint8Array(1),
                        new Uint8Array(2),
                        new Uint8Array(3),
                    ]
                },
            });
        });

        it('should deduplicate values included in the input', () => {
            expect(marshallItem(
                schema,
                {
                    binSet: [
                        Uint8Array.from([240, 159, 144, 142, 240, 159, 145, 177, 226, 157, 164]).buffer,
                        Uint8Array.from([240, 159, 144, 142, 240, 159, 145, 177, 226, 157, 164]),
                        '🐎👱❤',
                    ]
                }
            )).toEqual({
                binSet:{
                    BS: [
                        Uint8Array.from([240, 159, 144, 142, 240, 159, 145, 177, 226, 157, 164]),
                    ]
                },
            });
        });

        it('should remove empty values from sets', () => {
            expect(marshallItem(
                schema,
                {
                    binSet: new BinarySet([
                        new ArrayBuffer(0),
                        new ArrayBuffer(1),
                        new ArrayBuffer(2),
                        new ArrayBuffer(3),
                        new ArrayBuffer(0),
                    ])
                }
            )).toEqual({
                binSet: {
                    BS: [
                        new Uint8Array(1),
                        new Uint8Array(2),
                        new Uint8Array(3),
                    ]
                },
            });
        });

        it('should render empty sets as NullAttributeValues', () => {
            expect(marshallItem(schema, {binSet: [new ArrayBuffer(0)]}))
                .toEqual({
                    binSet: {NULL: true},
                });
        });
    });

    describe('boolean fields', () => {
        it('should marshall boolean fields', () => {
            const schema: Schema = {
                boolean: {type: 'Boolean'},
            };

            expect(marshallItem(schema, {boolean: false})).toEqual({
                boolean: {BOOL: false},
            });
        });
    });

    describe('custom fields', () => {
        it('should use the marshaller function embedded in the type', () => {
            const marshaller = jest.fn(() => ({S: 'stubbed'}));
            const schema = {
                custom: {
                    type: 'Custom',
                    marshall: marshaller,
                    unmarshall: jest.fn()
                } as CustomType<void>,
            };
            const document = {custom: 'a value'};
            expect(marshallItem(schema, document))
                .toEqual({custom: {S: 'stubbed'}});

            expect(marshaller.mock.calls.length).toBe(1);
            expect(marshaller.mock.calls[0][0]).toBe(document.custom);
        });
    });

    describe('collection fields', () => {
        it('should marshall iterables of untyped data', () => {
            const schema: Schema = {mixedList: {type: 'Collection'}};
            const input = {
                mixedList: [
                    'string',
                    123,
                    undefined,
                    new ArrayBuffer(12),
                    {foo: 'bar'},
                    ['one string', 234, new ArrayBuffer(5)],
                ]
            };

            expect(marshallItem(schema, input)).toEqual({
                mixedList: {
                    L: [
                        {S: 'string'},
                        {N: '123'},
                        {B: new ArrayBuffer(12)},
                        {M: {foo: {S: 'bar'}}},
                        {L: [
                            {S: 'one string'},
                            {N: '234'},
                            {B: new ArrayBuffer(5)},
                        ]},
                    ],
                },
            });
        });
    });

    describe('date fields', () => {
        const iso8601 = '2000-01-01T00:00:00Z';
        const epoch = 946684800;

        it('should marshall date objects', () => {
            const aDate = new Date(iso8601);
            const schema: Schema = {aDate: {type: 'Date'}};

            expect(marshallItem(schema, {aDate})).toEqual({
                aDate: {N: epoch.toString(10)},
            });
        });

        it('should marshall date strings', () => {
            const schema: Schema = {aDate: {type: 'Date'}};

            expect(marshallItem(schema, {aDate: iso8601})).toEqual({
                aDate: {N: epoch.toString(10)},
            });
        });

        it('should marshall numbers as epoch timestamps', () => {
            const schema: Schema = {aDate: {type: 'Date'}};

            expect(marshallItem(schema, {aDate: epoch})).toEqual({
                aDate: {N: epoch.toString(10)},
            });
        });

        it('should throw if an unexpected input is received', () => {
            const schema: Schema = {aDate: {type: 'Date'}};

            expect(() => marshallItem(schema, {aDate: new ArrayBuffer(10)}))
                .toThrow(objectContaining({invalidValue: new ArrayBuffer(10)}));
        });
    });

    describe('document fields', () => {
        it('should marshall documents as String => AttributeValue maps', () => {
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
            const input = {nested: {nested: {scalar: 'value'}}};

            expect(marshallItem(schema, input)).toEqual({
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
            });
        });
    });

    describe('hash fields', () => {
        it('should marshall objects of untyped data', () => {
            const schema: Schema = {mixedObject: {type: 'Hash'}};
            const input = {
                mixedObject: {
                    foo: 'string',
                    bar: 123,
                    baz: new ArrayBuffer(12),
                    fizz: {foo: 'bar'},
                    buzz: ['one string', 234, new Uint8Array(5)],
                    snap: new Set(['foo', 'foo', 'bar', 'bar', 'baz']),
                    crackle: new Set([0, 1, 2, 3, 0, 1, 2, 3]),
                    pop: new BinarySet([
                        new Uint8Array(1),
                        new Uint8Array(2),
                        new Uint8Array(3),
                    ])
                }
            };

            expect(marshallItem(schema, input)).toEqual({
                mixedObject: {
                    M: {
                        foo: {S: 'string'},
                        bar: {N: '123'},
                        baz: {B: new ArrayBuffer(12)},
                        fizz: {M: {foo: {S: 'bar'}}},
                        buzz: {
                            L: [
                                {S: 'one string'},
                                {N: '234'},
                                {B: new Uint8Array(5)},
                            ]
                        },
                        snap: {SS: ['foo', 'bar', 'baz']},
                        crackle: {NS: ['0', '1', '2', '3']},
                        pop: {BS: [
                            new Uint8Array(1),
                            new Uint8Array(2),
                            new Uint8Array(3),
                        ]}
                    },
                },
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

        it('should serialize an array of like items', () => {
            expect(marshallItem(schema, {list: ['a', 'b', 'c']})).toEqual({
                list: {
                    L: [
                        {S: 'a'},
                        {S: 'b'},
                        {S: 'c'},
                    ],
                },
            });
        });

        it('should serialize an iterable of like items', () => {
            const stringIterable = function *() {
                yield 'a';
                yield 'b';
                yield 'c';
            };

            expect(marshallItem(schema, {list: stringIterable()})).toEqual({
                list: {
                    L: [
                        {S: 'a'},
                        {S: 'b'},
                        {S: 'c'},
                    ],
                },
            });
        });

        it('should nullify empty members', () => {
            expect(marshallItem(schema, {list: ['a', '', 'c']})).toEqual({
                list: {
                    L: [
                        {S: 'a'},
                        {NULL: true},
                        {S: 'c'},
                    ],
                },
            });
        });
    });

    describe('map fields', () => {
        const schema: Schema = {
            map: {
                type: 'Map',
                memberType: {type: 'String'},
            },
        };

        it('should serialize an object with like values', () => {
            expect(marshallItem(schema, {map: {foo: 'bar', fizz: 'buzz'}}))
                .toEqual({
                    map: {
                        M: {
                            foo: {S: 'bar'},
                            fizz: {S: 'buzz'},
                        },
                    },
                });
        });

        it('should serialize a [string, ValueType] iterable', () => {
            const iterable = new Map<string, string>([
                ['foo', 'bar'],
                ['fizz', 'buzz'],
            ]);

            expect(marshallItem(schema, {map: iterable})).toEqual({
                map: {
                    M: {
                        foo: {S: 'bar'},
                        fizz: {S: 'buzz'},
                    },
                },
            });
        });

        it(
            'should throw if a value that cannot be converted to a map is received',
            () => {
                expect(() => marshallItem(schema, {map: 234})).toThrow();
            }
        );
    });

    describe('null fields', () => {
        it('should always return a null AttributeValue', () => {
            for (let value of ['string', 234, false, [], {}, new Int8Array(0)]) {
                expect(marshallItem({value: {type: 'Null'}}, {value}))
                    .toEqual({value: {NULL: true}});
            }
        });
    });

    describe('number fields', () => {
        it('should marshall number fields', () => {
            expect(marshallItem({num: {type: 'Number'}}, {num: 123}))
                .toEqual({num: {N: '123'}});
        });
    });

    describe('number set fields', () => {
        const schema: Schema = {
            numSet: { type: 'Set', memberType: 'Number'},
        };

        it('should serialize NumberSet fields', () => {
            expect(marshallItem(schema, {numSet: new Set([1, 2, 3])}))
                .toEqual({
                    numSet: {NS: ['1', '2', '3']},
                });
        });

        it('should deduplicate values included in the input', () => {
            expect(marshallItem(schema, {numSet: [1, 2, 3, 1]}))
                .toEqual({
                    numSet: {NS: ['1', '2', '3']},
                });
        });

        it('should render empty sets as NullAttributeValues', () => {
            expect(marshallItem(schema, {numSet: []}))
                .toEqual({
                    numSet: {NULL: true},
                });
        });
    });

    describe('set fields', () => {
        const schema: Schema = {
            fooSet: { type: 'Set', memberType: 'foo'} as any,
        };

        it('should throw an error if the memberType is not recognized', () => {
            expect(() => marshallItem(schema, {fooSet: [1, 2, 3, 1]}))
                .toThrowError(/Unrecognized set member type/);
        })
    });

    describe('string fields', () => {
        it('should marshall string fields', () => {
            expect(marshallItem({str: {type: 'String'}}, {str: 'string'}))
                .toEqual({str: {S: 'string'}});
        });

        it('should marshall stringable objects', () => {
            expect(marshallItem({str: {type: 'String'}}, {str: {}}))
                .toEqual({str: {S: '[object Object]'}});
        });

        it('should render empty strings as a NullAttributeValue', () => {
            expect(marshallItem({str: {type: 'String'}}, {str: ''}))
                .toEqual({str: {NULL: true}});
        });
    });

    describe('string set fields', () => {
        const schema: Schema = {
            strSet: { type: 'Set', memberType: 'String'},
        };

        it('should serialize StringSet fields', () => {
            expect(marshallItem(schema, {strSet: new Set(['a', 'b', 'c'])}))
                .toEqual({
                    strSet: {SS: ['a', 'b', 'c']},
                });
        });

        it('should deduplicate values included in the input', () => {
            expect(marshallItem(schema, {strSet: ['a', 'b', 'c', 'a']}))
                .toEqual({
                    strSet: {SS: ['a', 'b', 'c']},
                });
        });

        it('should remove empty values from sets', () => {
            expect(marshallItem(schema, {strSet: ['', 'a', 'b', 'c', '']}))
                .toEqual({
                    strSet: {SS: ['a', 'b', 'c']},
                });
        });

        it('should render empty sets as NullAttributeValues', () => {
            expect(marshallItem(schema, {strSet: ['', '']}))
                .toEqual({
                    strSet: {NULL: true},
                });
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

        it('should serialize Tuples', () => {
            expect(marshallItem(schema, {jobResult: [true, 123]})).toEqual({
                jobResult: {
                    L: [
                        {BOOL: true},
                        {N: '123'},
                    ],
                },
            });
        });
    });
});
