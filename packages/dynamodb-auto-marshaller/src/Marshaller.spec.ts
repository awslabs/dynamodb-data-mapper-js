import {Marshaller} from "./Marshaller";
import {BinarySet} from "./BinarySet";
import {NumberValue} from "./NumberValue";
import {NumberValueSet} from "./NumberValueSet";

describe('Marshaller', () => {
    describe('#marshallItem', () => {
        it('should convert objects to the DynamoDB item format', () => {
            const marshaller = new Marshaller();
            const marshalled = marshaller.marshallItem({
                string: 'foo',
                list: ['fizz', 'buzz', 'pop'],
                map: {
                    nestedMap: {
                        key: 'value',
                    }
                },
                number: 123,
                nullValue: null,
                boolValue: true,
                stringSet: new Set(['foo', 'bar', 'baz'])
            });

            expect(marshalled).toEqual({
                string: {S: 'foo'},
                list: {L: [{S: 'fizz'}, {S: 'buzz'}, {S: 'pop'}]},
                map: {
                    M: {
                        nestedMap: {
                            M: {
                                key: {S: 'value'}
                            }
                        }
                    }
                },
                number: {N: '123'},
                nullValue: {NULL: true},
                boolValue: {BOOL: true},
                stringSet: {SS: ['foo', 'bar', 'baz']}
            });
        });

        it(
            'should return an empty attribute map when provided invalid input and the onInvalid option is set to "omit"',
            () => {
                const marshaller = new Marshaller({onInvalid: "omit"});
                expect(marshaller.marshallItem('string' as any)).toEqual({});
            }
        );

        it('should throw when provided invalid input and the onInvalid option is set to "throw"', () => {
            const marshaller = new Marshaller({onInvalid: 'throw'});
            expect(() => marshaller.marshallItem('string' as any)).toThrow();
        });
    });

    describe('#marshallValue', () => {
        describe('strings', () => {
            it('should convert strings to StringAttributeValues', () => {
                expect((new Marshaller()).marshallValue('string'))
                    .toEqual({S: 'string'});
            });

            it(
                'should convert empty strings to null when onEmpty option set to "nullify"',
                () => {
                    expect(
                        (new Marshaller({onEmpty: "nullify"})).marshallValue('')
                    ).toEqual({NULL: true});
                }
            );

            it(
                'should remove empty strings when onEmpty option set to "omit"',
                () => {
                    expect(
                        (new Marshaller({onEmpty: "omit"})).marshallValue('')
                    ).toBeUndefined();
                }
            );

            it(
                'should convert empty strings to StringAttributeValues otherwise',
                () => {
                    expect((new Marshaller()).marshallValue(''))
                        .toEqual({S: ''});
                }
            );
        });

        describe('binary values', () => {
            it('should convert binary values to BinaryAttributeValues', () => {
                const bin = Uint8Array.from([0xde, 0xad, 0xbe, 0xef]);
                expect((new Marshaller()).marshallValue(bin))
                    .toEqual({B: bin});
            });

            it(
                'should convert empty binary values to null when onEmpty option set to "nullify"',
                () => {
                    expect(
                        (new Marshaller({onEmpty: "nullify"}))
                            .marshallValue(new Uint8Array(0))
                    ).toEqual({NULL: true});
                }
            );

            it(
                'should omit empty binary values when onEmpty option set to "omit"',
                () => {
                    expect(
                        (new Marshaller({onEmpty: "omit"}))
                            .marshallValue(new Uint8Array(0))
                    ).toBeUndefined();
                }
            );

            it(
                'should convert empty binary values to null when onEmpty option set to "nullify"',
                () => {
                    expect(
                        (new Marshaller()).marshallValue(new Uint8Array(0))
                    ).toEqual({B: new Uint8Array(0)});
                }
            );
        });

        describe('numbers', () => {
            it('should convert numbers to NumberAttributeValues', () => {
                expect((new Marshaller()).marshallValue(42))
                    .toEqual({N: '42'});
            });

            it('should convert NumberValues to NumberAttributeValues', () => {
                expect(
                    (new Marshaller()).marshallValue(new NumberValue('123'))
                ).toEqual({N: '123'});
            });
        });

        describe('null', () => {
            it('should convert nulls to NullAttributeValues', () => {
                expect((new Marshaller()).marshallValue(null))
                    .toEqual({NULL: true});
            });
        });

        describe('boolean', () => {
            it('should convert booleans to BooleanAttributeValues', () => {
                const marshaller = new Marshaller();
                expect(marshaller.marshallValue(true)).toEqual({BOOL: true});
                expect(marshaller.marshallValue(false)).toEqual({BOOL: false});
            });
        });

        describe('lists', () => {
            it('should convert arrays to ListAttributeValues', () => {
                expect((new Marshaller()).marshallValue([])).toEqual({L: []});
            });

            it('should convert list members to AttributeValues', function() {
                expect(
                    (new Marshaller()).marshallValue(['a', 1, true, null, {}])
                ).toEqual({L: [
                    {S: 'a'},
                    {N: '1'},
                    {BOOL: true},
                    {NULL: true},
                    {M: {}},
                ]});
            });

            it('should convert iterables to ListAttributeValues', () => {
                const inputGen = function *() {
                    yield 'a';
                    yield 1;
                    yield true;
                    yield null;
                    yield {};
                };

                expect(
                    (new Marshaller()).marshallValue(inputGen())
                ).toEqual({L: [
                    {S: 'a'},
                    {N: '1'},
                    {BOOL: true},
                    {NULL: true},
                    {M: {}},
                ]});
            });

            it('should omit undefined values from the serialized list', () => {
                expect(
                    (new Marshaller())
                        .marshallValue([
                            'a',
                            undefined,
                            1,
                            undefined,
                            true,
                            undefined,
                            null,
                            undefined,
                            {}
                        ])
                ).toEqual({L: [
                    {S: 'a'},
                    {N: '1'},
                    {BOOL: true},
                    {NULL: true},
                    {M: {}},
                ]});
            });
        });

        describe('maps', () => {
            it('should convert objects to MapAttributeValues', () => {
                expect((new Marshaller()).marshallValue({})).toEqual({M: {}});
            });

            it('should convert maps to MapAttributeValues', () => {
                expect((new Marshaller()).marshallValue(new Map()))
                    .toEqual({M: {}});
            });

            it(
                'should omit keys whose values are serialized as undefined',
                () => {
                    const marshaller = new Marshaller();
                    expect(marshaller.marshallValue({a: void 0}))
                        .toEqual({M: {}});

                    expect(marshaller.marshallValue(new Map([['a', void 0]])))
                        .toEqual({M: {}});
                }
            );

            it(
                'should convert objects with inheritance chains to MapAttributeValues',
                () => {
                    class MyPrototype {
                        public readonly foo: string = 'bar';
                    }

                    class MyDescendant extends MyPrototype {
                        public readonly fizz: string = 'buzz';
                    }

                    const myInstance = new MyDescendant();
                    (myInstance as any).quux = true;

                    expect((new Marshaller()).marshallValue(myInstance))
                        .toEqual({
                            M: {
                                foo: {S: 'bar'},
                                fizz: {S: 'buzz'},
                                quux: {BOOL: true}
                            }
                        });
                }
            );

            it('should convert map members to AttributeValues', () => {
                const map = new Map<string, any>();
                map.set('a', 'a');
                map.set('b', 1);
                map.set('c', true);
                map.set('d', null);
                map.set('e', ['s']);

                expect((new Marshaller()).marshallValue(map)).toEqual({
                    M: {
                        a: {S: 'a'},
                        b: {N: '1'},
                        c: {BOOL: true},
                        d: {NULL: true},
                        e: {L: [{S: 's'}]}
                    }
                });
            });

            it(
                'should omit map members whose keys are not strings when the onInvalid option is "omit"',
                () => {
                    const marshaller = new Marshaller({onInvalid: "omit"});
                    const map = new Map<any, any>();
                    map.set('a', 'a');
                    map.set(1, 1);
                    map.set({}, true);
                    map.set([], null);
                    map.set(null, ['s']);

                    expect(marshaller.marshallValue(map))
                        .toEqual({M: {a: {S: 'a'}}});
                }
            );

            it('should throw otherwise', () => {
                const marshaller = new Marshaller();
                const map = new Map<any, any>();
                map.set('a', 'a');
                map.set(1, 1);
                map.set({}, true);
                map.set([], null);
                map.set(null, ['s']);

                expect(() => marshaller.marshallValue(map)).toThrow();
            });
        });

        describe('sets', () => {
            it(
                'should omit empty sets when the onEmpty option is "omit"',
                () => {
                    const marshaller = new Marshaller({onEmpty: "omit"});
                    expect(marshaller.marshallValue(new Set()))
                        .toBeUndefined();
                }
            );

            it(
                'should convert empty sets to null when the onEmpty option is "nullify"',
                () => {
                    const marshaller = new Marshaller({onEmpty: "nullify"});
                    expect(marshaller.marshallValue(new Set()))
                        .toEqual({NULL: true});
                }
            );
            it(
                'should omit empty sets when the onEmpty option is "leave", as the kind of set cannot be inferred',
                () => {
                    const marshaller = new Marshaller({onEmpty: "leave"});
                    expect(marshaller.marshallValue(new Set()))
                        .toBeUndefined();
                }
            );

            it(
                'should omit sets with members of an unknown type when the onEmpty option is "omit"',
                () => {
                    const marshaller = new Marshaller({onInvalid: "omit"});
                    const set = new Set<object>();
                    set.add({});
                    expect(marshaller.marshallValue(set))
                        .toBeUndefined();
                }
            );

            it(
                'should throw on sets with members of an unknown type otherwise',
                () => {
                    const marshaller = new Marshaller();
                    const set = new Set<object>();
                    set.add({});
                    expect(() => marshaller.marshallValue(set)).toThrow();
                }
            );

            it(
                'should drop invalid members when onInvalid option is set to "omit"',
                () => {
                    const marshaller = new Marshaller({onInvalid: "omit"});
                    expect(marshaller.marshallValue(new Set(['a', 1, 'c'])))
                        .toEqual({SS: ['a', 'c']});
                }
            );

            it('should throw on invalid members otherwise', () => {
                const marshaller = new Marshaller();
                expect(
                    () => marshaller.marshallValue(new Set(['a', 1, 'c']))
                ).toThrow();
            });

            it(
                'should return a NullAttributeValue for an emptied set when onEmpty is set to "nullify"',
                () => {
                    const marshaller = new Marshaller({onEmpty: "nullify"});
                    expect(marshaller.marshallValue(new Set([''])))
                        .toEqual({NULL: true});
                }
            );

            it(
                'should return undefined for an emptied set when onEmpty is set to "omit"',
                () => {
                    const marshaller = new Marshaller({onEmpty: "omit"});
                    expect(marshaller.marshallValue(new Set([''])))
                        .toBeUndefined();
                }
            );

            it('should serialize empty values otherwise', () => {
                const marshaller = new Marshaller();
                expect(marshaller.marshallValue(new Set([''])))
                    .toEqual({SS: ['']});
            });

            describe('string sets', () => {
                it(
                    'should convert sets with strings into StringSetAttributeValues',
                    () => {
                        expect(
                            (new Marshaller())
                                .marshallValue(new Set<string>(['a', 'b', 'c']))
                        ).toEqual({SS: ['a', 'b', 'c']});
                    }
                );

                it(
                    'should drop empty members when onEmpty option is set to "nullify"',
                    () => {
                        expect(
                            (new Marshaller({onEmpty: 'nullify'}))
                                .marshallValue(new Set<string>(['a', '', 'c']))
                        ).toEqual({SS: ['a', 'c']});
                    }
                );

                it(
                    'should drop empty members when onEmpty option is set to "omit"',
                    () => {
                        expect(
                            (new Marshaller({onEmpty: 'omit'}))
                                .marshallValue(new Set<string>(['a', '', 'c']))
                        ).toEqual({SS: ['a', 'c']});
                    }
                );

                it('should keep empty members otherwise', () => {
                    expect(
                        (new Marshaller())
                            .marshallValue(new Set<string>(['a', '', 'c']))
                    ).toEqual({SS: ['a', '', 'c']});
                });
            });

            describe('number sets', () => {
                it(
                    'should convert sets with numbers into NumberSetAttributeValues',
                    () => {
                        expect(
                            (new Marshaller())
                                .marshallValue(new Set<number>([1, 2, 3]))
                        ).toEqual({NS: ['1', '2', '3']});
                    }
                );

                it(
                    'should convert NumberValueSet objects into NumberSetAttributeValues',
                    () => {
                        expect(
                            (new Marshaller())
                                .marshallValue(new NumberValueSet([
                                    new NumberValue('1'),
                                    new NumberValue('2'),
                                    new NumberValue('3'),
                                ]))
                        ).toEqual({NS: ['1', '2', '3']});
                    }
                );
            });

            describe('binary sets', () => {
                it(
                    'should convert sets with binary values into BinarySetAttributeValues',
                    () => {
                        const marshaller = new Marshaller();
                        const converted = marshaller.marshallValue(new BinarySet([
                            Uint8Array.from([0xde, 0xad]),
                            Uint8Array.from([0xbe, 0xef]).buffer,
                            Uint8Array.from([0xfa, 0xce]),
                        ]));
                        expect(converted).toEqual({BS: [
                            Uint8Array.from([0xde, 0xad]),
                            Uint8Array.from([0xbe, 0xef]).buffer,
                            Uint8Array.from([0xfa, 0xce]),
                        ]});
                    }
                );

                it(
                    'should drop empty members when the onEmpty option is set to "nullify"',
                    () => {
                        const marshaller = new Marshaller({onEmpty: 'nullify'});
                        const converted = marshaller.marshallValue(new BinarySet([
                            Uint8Array.from([0xde, 0xad]),
                            Uint8Array.from([0xbe, 0xef]).buffer,
                            Uint8Array.from([0xfa, 0xce]),
                            new Uint8Array(0),
                        ]));
                        expect(converted).toEqual({BS: [
                            Uint8Array.from([0xde, 0xad]),
                            Uint8Array.from([0xbe, 0xef]).buffer,
                            Uint8Array.from([0xfa, 0xce]),
                        ]});
                    }
                );

                it(
                    'should drop empty members when the onEmpty option is set to "omit"',
                    () => {
                        const marshaller = new Marshaller({onEmpty: 'omit'});
                        const converted = marshaller.marshallValue(new BinarySet([
                            Uint8Array.from([0xde, 0xad]),
                            Uint8Array.from([0xbe, 0xef]).buffer,
                            Uint8Array.from([0xfa, 0xce]),
                            new Uint8Array(0),
                        ]));
                        expect(converted).toEqual({BS: [
                            Uint8Array.from([0xde, 0xad]),
                            Uint8Array.from([0xbe, 0xef]).buffer,
                            Uint8Array.from([0xfa, 0xce]),
                        ]});
                    }
                );

                it('should keep empty members otherwise', () => {
                    const marshaller = new Marshaller();
                    const converted = marshaller.marshallValue(new BinarySet([
                        Uint8Array.from([0xde, 0xad]),
                        Uint8Array.from([0xbe, 0xef]).buffer,
                        Uint8Array.from([0xfa, 0xce]),
                        new Uint8Array(0),
                    ]));
                    expect(converted).toEqual({BS: [
                        Uint8Array.from([0xde, 0xad]),
                        Uint8Array.from([0xbe, 0xef]).buffer,
                        Uint8Array.from([0xfa, 0xce]),
                        new Uint8Array(0),
                    ]});
                });
            });
        });

        describe('undefined values', () => {
            it('should return undefined for undefined', () => {
                expect((new Marshaller().marshallValue(void 0)))
                    .toBeUndefined();
            });
        });

        describe('symbols', () => {
            it(
                'should omit symbols when the onInvalid option is set to "omit"',
                () => {
                    expect(
                        (new Marshaller({onInvalid: "omit"})
                            .marshallValue(Symbol.iterator))
                    ).toBeUndefined();
                }
            );

            it('should throw on symbols otherwise', () => {
                expect(
                    () => (new Marshaller().marshallValue(Symbol.iterator))
                ).toThrow();
            });
        });

        describe('functions', () => {
            it(
                'should omit functions when the onInvalid option is set to "omit"',
                () => {
                    expect(
                        (new Marshaller({onInvalid: "omit"})
                            .marshallValue(() => {}))
                    ).toBeUndefined();
                }
            );

            it('should throw on symbols otherwise', () => {
                expect(
                    () => (new Marshaller().marshallValue(() => {}))
                ).toThrow();
            });
        });
    });

    describe('#unmarshallItem', () => {
        it('should convert DynamoDB items to plain vanilla JS objects', function() {
            var unmarshalled = (new Marshaller({unwrapNumbers: true})).unmarshallItem({
                string: {S: 'foo'},
                list: {L: [{S: 'fizz'}, {S: 'buzz'}, {S: 'pop'}]},
                map: {
                    M: {
                        nestedMap: {
                            M: {
                                key: {S: 'value'}
                            }
                        }
                    }
                },
                number: {N: '123'},
                nullValue: {NULL: true},
                boolValue: {BOOL: true}
            });

            expect(unmarshalled).toEqual({
                string: 'foo',
                list: ['fizz', 'buzz', 'pop'],
                map: {
                    nestedMap: {
                        key: 'value',
                    }
                },
                number: 123,
                nullValue: null,
                boolValue: true
            });
        });
    });

    describe('#unmarshallValue', () => {
        const marshaller = new Marshaller();
        describe('strings', () => {
            it('should convert StringAttributeValues to strings', () => {
                expect(marshaller.unmarshallValue({S: 'string'}))
                    .toEqual('string');
            });
        });

        describe('binary values', () => {
            it('should convert BinaryAttributeValues to binary values', () => {
                expect(marshaller.unmarshallValue({B: new Uint8Array(1)}))
                    .toEqual(new Uint8Array(1));
            });
        });

        describe('numbers', () => {
            it(
                'should convert NumberAttributeValues to NumberValues',
                () => {
                    const unsafeInteger = '9007199254740991000';
                    const converted = marshaller.unmarshallValue({N: unsafeInteger}) as NumberValue;
                    expect(converted.toString()).toBe(unsafeInteger);
                }
            );

            it(
                'should convert NumberAttributeValues to numbers when unwrapNumbers is true',
                () => {
                    const marshaller = new Marshaller({unwrapNumbers: true});
                    expect(marshaller.unmarshallValue({N: '42'})).toEqual(42);
                }
            );
        });

        describe('null', () => {
            it('should convert NullAttributeValues to null', () => {
                expect(marshaller.unmarshallValue({NULL: true})).toEqual(null);
            });
        });

        describe('boolean', () => {
            it('should convert BooleanAttributeValues to booleans', () => {
                expect(marshaller.unmarshallValue({BOOL: true})).toEqual(true);
                expect(marshaller.unmarshallValue({BOOL: false}))
                    .toEqual(false);
            });
        });

        describe('lists', () => {
            it('should convert ListAttributeValues to lists', () => {
                expect(marshaller.unmarshallValue({L: []})).toEqual([]);
            });

            it('should convert member AttributeValues to list members', () => {
                expect(marshaller.unmarshallValue({L: [
                    {S: 'a'},
                    {N: '1'},
                    {BOOL: true},
                    {NULL: true},
                    {M: {}}
                ]})).toEqual(['a', new NumberValue('1'), true, null, {}]);
            });
        });

        describe('maps', () => {
            it('should convert MapAttributeValues to objects', () => {
                expect(marshaller.unmarshallValue({M: {}})).toEqual({});
            });

            it('should convert member AttributeValues to map members', () => {
                expect(marshaller.unmarshallValue({
                    M: {
                        a: {S: 'a'},
                        b: {N: '1'},
                        c: {BOOL: true},
                        d: {NULL: true},
                        e: {L: [{S: 's'}]}
                    }
                })).toEqual({
                    a: 'a',
                    b: new NumberValue('1'),
                    c: true,
                    d: null,
                    e: ['s'],
                });
            });
        });

        describe('string sets', () => {
            it(
                'should convert StringSetAttributeValues into sets with strings',
                () => {
                    expect(marshaller.unmarshallValue({SS: ['a', 'b', 'c']}))
                        .toEqual(new Set(['a', 'b', 'c']));
                }
            );
        });

        describe('number sets', () => {
            it(
                'should convert NumberSetAttributeValues into sets with NumberValues',
                function() {
                    const unsafeInteger = '900719925474099100';
                    const converted = marshaller.unmarshallValue({NS: [
                        unsafeInteger + '1',
                        unsafeInteger + '2',
                        unsafeInteger + '3',
                    ]});

                    expect(converted).toEqual(new NumberValueSet([
                        new NumberValue(unsafeInteger + '1'),
                        new NumberValue(unsafeInteger + '2'),
                        new NumberValue(unsafeInteger + '3'),
                    ]));
                }
            );

            it(
                'should convert NumberSetAttributeValues into sets with numbers when unwrapNumbers is true',
                () => {
                    const marshaller = new Marshaller({unwrapNumbers: true});
                    expect(marshaller.unmarshallValue({NS: ['1', '2', '3']}))
                        .toEqual(new Set([1, 2, 3]));
                }
            );
        });

        describe('binary sets', () => {
            it(
                'should convert BinarySetAttributeValues into sets with binary strings',
                () => {
                    expect(
                        marshaller.unmarshallValue({BS: [
                            new Uint8Array(1),
                            new Uint8Array(2),
                        ]})
                    ).toEqual(new BinarySet([
                        new Uint8Array(1),
                        new Uint8Array(2),
                    ]));
                }
            );
        });

        it('should convert objects with no values to empty maps', () => {
            expect(marshaller.unmarshallValue({foo: 'bar'} as any))
                .toEqual({});
        });
    });
});
