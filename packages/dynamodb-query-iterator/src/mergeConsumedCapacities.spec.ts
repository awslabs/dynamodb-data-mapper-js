import { mergeConsumedCapacities } from './mergeConsumedCapacities';
import { ConsumedCapacity } from 'aws-sdk/clients/dynamodb';

describe('mergeConsumedCapacities', () => {
    it('should return undefined when called two undefined arguments', () => {
        expect(mergeConsumedCapacities(void 0, void 0)).toBeUndefined();
    });

    it('should throw when called with capacities from two different tables', () => {
        expect(
            () => mergeConsumedCapacities({TableName: 'foo'}, {TableName: 'bar'})
        ).toThrow();
    });

    it(
        'should return a clone of the first argument when the second is undefined',
        () => {
            const capacity: ConsumedCapacity = {
                TableName: 'foo',
                CapacityUnits: 2,
                Table: {
                    CapacityUnits: 4,
                },
                LocalSecondaryIndexes: {
                    foo: {
                        CapacityUnits: 6
                    }
                },
                GlobalSecondaryIndexes: {
                    bar: {
                        CapacityUnits: 8
                    }
                }
            };
            const merged = mergeConsumedCapacities(capacity, void 0);
            expect(merged).toEqual(capacity);
            expect(merged).not.toBe(capacity);
        }
    );

    it(
        'should return a clone of the second argument when the first is undefined',
        () => {
            const capacity: ConsumedCapacity = {
                TableName: 'foo',
                CapacityUnits: 2,
                Table: {
                    CapacityUnits: 4,
                },
                LocalSecondaryIndexes: {
                    foo: {
                        CapacityUnits: 6
                    }
                },
                GlobalSecondaryIndexes: {
                    bar: {
                        CapacityUnits: 8
                    }
                }
            };
            const merged = mergeConsumedCapacities(void 0, capacity);
            expect(merged).toEqual(capacity);
            expect(merged).not.toBe(capacity);
        }
    );

    it(
        'should return a clone of the first argument when the second is undefined',
        () => {
            const a: ConsumedCapacity = {
                TableName: 'foo',
                CapacityUnits: 2,
                Table: {
                    CapacityUnits: 4,
                },
                LocalSecondaryIndexes: {
                    foo: {
                        CapacityUnits: 6
                    },
                    fizz: {
                        CapacityUnits: 2
                    }
                },
                GlobalSecondaryIndexes: {
                    bar: {
                        CapacityUnits: 8
                    },
                    buzz: {
                        CapacityUnits: 2
                    }
                }
            };
            const b: ConsumedCapacity = {
                TableName: 'foo',
                CapacityUnits: 2,
                Table: {
                    CapacityUnits: 4,
                },
                LocalSecondaryIndexes: {
                    foo: {
                        CapacityUnits: 6
                    },
                    snap: {
                        CapacityUnits: 2
                    }
                },
                GlobalSecondaryIndexes: {
                    bar: {
                        CapacityUnits: 8
                    },
                    crackle: {
                        CapacityUnits: 2
                    }
                }
            };

            expect(mergeConsumedCapacities(a, b)).toEqual({
                TableName: 'foo',
                CapacityUnits: a.CapacityUnits! + b.CapacityUnits!,
                Table: {
                    CapacityUnits: a.Table!.CapacityUnits! + b.Table!.CapacityUnits!,
                },
                LocalSecondaryIndexes: {
                    foo: {
                        CapacityUnits: a.LocalSecondaryIndexes!.foo.CapacityUnits!
                            + b.LocalSecondaryIndexes!.foo.CapacityUnits!
                    },
                    fizz: {
                        CapacityUnits: a.LocalSecondaryIndexes!.fizz.CapacityUnits
                    },
                    snap: {
                        CapacityUnits: b.LocalSecondaryIndexes!.snap.CapacityUnits
                    }
                },
                GlobalSecondaryIndexes: {
                    bar: {
                        CapacityUnits: a.GlobalSecondaryIndexes!.bar.CapacityUnits!
                        + b.GlobalSecondaryIndexes!.bar.CapacityUnits!
                    },
                    buzz: {
                        CapacityUnits: a.GlobalSecondaryIndexes!.buzz.CapacityUnits
                    },
                    crackle: {
                        CapacityUnits: b.GlobalSecondaryIndexes!.crackle.CapacityUnits
                    }
                }

            });
        }
    );
});
