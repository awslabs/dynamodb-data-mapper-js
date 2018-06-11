import { QueryIterator } from '.';

describe('QueryIterator', () => {
    const promiseFunc = jest.fn();
    const mockDynamoDbClient = {
        config: {},
        query: jest.fn()
    };

    beforeEach(() => {
        promiseFunc.mockClear();
        promiseFunc.mockImplementation(() => Promise.resolve({Items: []}));
        mockDynamoDbClient.query.mockClear();
        mockDynamoDbClient.query.mockImplementation(() => {
            return {promise: promiseFunc};
        });
    });

    it(
        'should paginate over results and return a promise for each item',
        async () => {
            promiseFunc.mockImplementationOnce(() => Promise.resolve({
                Items: [
                    {
                        fizz: {S: 'snap'},
                        bar: {NS: ['1', '2', '3']},
                        baz: {L: [{BOOL: true}, {N: '4'}]}
                    },
                ],
                LastEvaluatedKey: {fizz: {S: 'snap'}},
            }));
            promiseFunc.mockImplementationOnce(() => Promise.resolve({
                Items: [
                    {
                        fizz: {S: 'crackle'},
                        bar: {NS: ['5', '6', '7']},
                        baz: {L: [{BOOL: false}, {N: '8'}]}
                    },
                ],
                LastEvaluatedKey: {fizz: {S: 'crackle'}},
            }));
            promiseFunc.mockImplementationOnce(() => Promise.resolve({
                Items: [
                    {
                        fizz: {S: 'pop'},
                        bar: {NS: ['9', '12', '30']},
                        baz: {L: [{BOOL: true}, {N: '24'}]}
                    },
                ],
                LastEvaluatedKey: {fizz: {S: 'pop'}},
            }));
            promiseFunc.mockImplementationOnce(() => Promise.resolve({}));

            const result: any[] = [];
            for await (const item of new QueryIterator(mockDynamoDbClient as any, {TableName: 'foo'}, ['fizz'])) {
                result.push(item);
            }

            expect(result).toEqual([
                {
                    fizz: {S: 'snap'},
                    bar: {NS: ['1', '2', '3']},
                    baz: {L: [{BOOL: true}, {N: '4'}]}
                },
                {
                    fizz: {S: 'crackle'},
                    bar: {NS: ['5', '6', '7']},
                    baz: {L: [{BOOL: false}, {N: '8'}]}
                },
                {
                    fizz: {S: 'pop'},
                    bar: {NS: ['9', '12', '30']},
                    baz: {L: [{BOOL: true}, {N: '24'}]}
                },
            ]);
        }
    );

    it('should provide access to the last evaluated key', async () => {
        promiseFunc.mockImplementationOnce(() => Promise.resolve({
            Items: [
                {
                    fizz: {S: 'snap'},
                    bar: {NS: ['1', '2', '3']},
                    baz: {L: [{BOOL: true}, {N: '4'}]}
                },
                {
                    fizz: {S: 'crackle'},
                    bar: {NS: ['5', '6', '7']},
                    baz: {L: [{BOOL: false}, {N: '8'}]}
                },
                {
                    fizz: {S: 'pop'},
                    bar: {NS: ['9', '12', '30']},
                    baz: {L: [{BOOL: true}, {N: '24'}]}
                },
            ],
            LastEvaluatedKey: {fizz: {S: 'pop'}},
        }));
        promiseFunc.mockImplementationOnce(() => Promise.resolve({}));

        const iterator = new QueryIterator(mockDynamoDbClient as any, {TableName: 'foo'}, ['fizz']);

        // lastEvaluatedKey should be undefined before iteration starts
        expect(iterator.lastEvaluatedKey).toBeUndefined();

        const expectedLastKeys = [
            {fizz: {S: 'snap'}},
            {fizz: {S: 'crackle'}},
            {fizz: {S: 'pop'}},
        ];

        for await (const _ of iterator) {
            expect(iterator.lastEvaluatedKey).toEqual(expectedLastKeys.shift());
        }

        expect(iterator.lastEvaluatedKey).toBeUndefined();
    });

    it('should provide access to paginator metadata', async () => {
        promiseFunc.mockImplementationOnce(() => Promise.resolve({
            Items: [
                {
                    fizz: {S: 'snap'},
                    bar: {NS: ['1', '2', '3']},
                    baz: {L: [{BOOL: true}, {N: '4'}]}
                },
            ],
            LastEvaluatedKey: {fizz: {S: 'snap'}},
            Count: 1,
            ScannedCount:1,
            ConsumedCapacity: {
                TableName: 'foo',
                CapacityUnits: 2
            }
        }));
        promiseFunc.mockImplementationOnce(() => Promise.resolve({
            Items: [
                {
                    fizz: {S: 'crackle'},
                    bar: {NS: ['5', '6', '7']},
                    baz: {L: [{BOOL: false}, {N: '8'}]}
                },
            ],
            LastEvaluatedKey: {fizz: {S: 'crackle'}},
            Count: 1,
            ScannedCount: 2,
            ConsumedCapacity: {
                TableName: 'foo',
                CapacityUnits: 2
            }
        }));
        promiseFunc.mockImplementationOnce(() => Promise.resolve({
            Items: [
                {
                    fizz: {S: 'pop'},
                    bar: {NS: ['9', '12', '30']},
                    baz: {L: [{BOOL: true}, {N: '24'}]}
                },
            ],
            Count: 1,
            ScannedCount: 3,
            ConsumedCapacity: {
                TableName: 'foo',
                CapacityUnits: 2
            }
        }));

        const iterator = new QueryIterator(mockDynamoDbClient as any, {TableName: 'foo'}, ['fizz']);

        let expectedCount = 0;
        const expectedScanCounts = [1, 3, 6];
        expect(iterator.count).toBe(expectedCount);
        expect(iterator.scannedCount).toBe(expectedCount);
        for await (const _ of iterator) {
            expect(iterator.count).toBe(++expectedCount);
            expect(iterator.scannedCount).toBe(expectedScanCounts.shift());
        }

        expect(iterator.count).toBe(3);
        expect(iterator.scannedCount).toBe(6);
        expect(iterator.consumedCapacity).toEqual({
            TableName: 'foo',
            CapacityUnits: 6
        });
    });

    it(
        'should report the last evaluated key even after ceasing iteration',
        async () => {
            promiseFunc.mockImplementationOnce(() => Promise.resolve({
                Items: [
                    {
                        fizz: {S: 'snap'},
                        bar: {NS: ['1', '2', '3']},
                        baz: {L: [{BOOL: true}, {N: '4'}]}
                    },
                    {
                        fizz: {S: 'crackle'},
                        bar: {NS: ['5', '6', '7']},
                        baz: {L: [{BOOL: false}, {N: '8'}]}
                    },
                    {
                        fizz: {S: 'pop'},
                        bar: {NS: ['9', '12', '30']},
                        baz: {L: [{BOOL: true}, {N: '24'}]}
                    },
                ],
                LastEvaluatedKey: {fizz: {S: 'pop'}},
            }));
            promiseFunc.mockImplementationOnce(() => Promise.resolve({}));

            const iterator = new QueryIterator(mockDynamoDbClient as any, {TableName: 'foo'}, ['fizz']);
            for await (const _ of iterator) {
                break;
            }

            expect(iterator.lastEvaluatedKey).toEqual({fizz: {S: 'snap'}});
        }
    );
});
