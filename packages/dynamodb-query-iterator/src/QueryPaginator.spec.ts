import { QueryPaginator } from '.';

describe('QueryPaginator', () => {
    const promiseFunc = jest.fn();
    const mockDynamoDbClient = {
        config: {},
        query: jest.fn()
    };

    beforeEach(() => {
        promiseFunc.mockClear();
        promiseFunc.mockImplementation(() => Promise.resolve({Items: []}));
        mockDynamoDbClient.query.mockClear();
        mockDynamoDbClient.query.mockImplementation(promiseFunc);
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
            for await (const res of new QueryPaginator(mockDynamoDbClient as any, {TableName: 'foo'})) {
                result.push(...res.Items || []);
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

    it('should fetch up to $limit records', async () => {
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
            ],
            LastEvaluatedKey: {fizz: {S: 'crackle'}},
        }));

        const paginator = new QueryPaginator(mockDynamoDbClient as any, {TableName: 'foo'}, 2);
        const result: any[] = [];
        for await (const res of paginator) {
            result.push(...res.Items || []);
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
            }
        ]);

        expect(paginator.lastEvaluatedKey).toEqual({fizz: {S: 'crackle'}});
    });

    it('should not request a page size that will exceed $limit', async () => {
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
            ],
            LastEvaluatedKey: {fizz: {S: 'crackle'}},
        }));
        promiseFunc.mockImplementationOnce(() => Promise.resolve({}));

        const paginator = new QueryPaginator(mockDynamoDbClient as any, {TableName: 'foo'}, 3);
        for await (const _ of paginator) {
            // pass
        }

        expect(mockDynamoDbClient.query.mock.calls).toEqual([
            [{TableName: 'foo', Limit: 3}],
            [{
                TableName: 'foo',
                Limit: 1,
                ExclusiveStartKey: {fizz: {S: 'crackle'}}
            }],
        ]);
    });

    it('should provide access to the last evaluated key', async () => {
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

        const paginator = new QueryPaginator(mockDynamoDbClient as any, {TableName: 'foo'});
        const expectedLastKeys = [
            {fizz: {S: 'snap'}},
            {fizz: {S: 'crackle'}},
            {fizz: {S: 'pop'}},
        ];

        for await (const _ of paginator) {
            expect(paginator.lastEvaluatedKey).toEqual(expectedLastKeys.shift());
        }

        expect(paginator.lastEvaluatedKey).toBeUndefined();
    });

    it('should merge counts', async () => {
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
            ScannedCount:1
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
            ScannedCount: 3
        }));

        const paginator = new QueryPaginator(mockDynamoDbClient as any, {TableName: 'foo'});

        let expectedCount = 0;
        const expectedScanCounts = [1, 3, 6];
        expect(paginator.count).toBe(expectedCount);
        expect(paginator.scannedCount).toBe(expectedCount);
        for await (const _ of paginator) {
            expect(paginator.count).toBe(++expectedCount);
            expect(paginator.scannedCount).toBe(expectedScanCounts.shift());
        }

        expect(paginator.count).toBe(3);
        expect(paginator.scannedCount).toBe(6);
    });

    it('should merge consumed capacity reports', async () => {
        promiseFunc.mockImplementationOnce(() => Promise.resolve({
            Items: [
                {
                    fizz: {S: 'snap'},
                    bar: {NS: ['1', '2', '3']},
                    baz: {L: [{BOOL: true}, {N: '4'}]}
                },
            ],
            LastEvaluatedKey: {fizz: {S: 'snap'}},
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
            ConsumedCapacity: {
                TableName: 'foo',
                CapacityUnits: 2
            }
        }));

        const paginator = new QueryPaginator(mockDynamoDbClient as any, {TableName: 'foo'});

        for await (const _ of paginator) {
            // pass
        }
        expect(paginator.consumedCapacity).toEqual({
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

            let i = 0;
            const paginator = new QueryPaginator(mockDynamoDbClient as any, {TableName: 'foo'});
            for await (const _ of paginator) {
                if (++i > 1) {
                    break;
                }
            }

            expect(paginator.lastEvaluatedKey).toEqual({fizz: {S: 'crackle'}});
        }
    );
});
