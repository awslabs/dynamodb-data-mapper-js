import { ParallelScanIterator } from '.';

describe('ParallelScanIterator', () => {
    const promiseFunc = jest.fn();
    const mockDynamoDbClient = {
        config: {},
        scan: jest.fn()
    };

    beforeEach(() => {
        promiseFunc.mockClear();
        promiseFunc.mockImplementation(() => Promise.resolve({Items: []}));
        mockDynamoDbClient.scan.mockClear();
        mockDynamoDbClient.scan.mockImplementation(() => {
            return {promise: promiseFunc};
        });
    });

    it(
        'should paginate over results and return a promise for each item',
        async () => {
            const segments = 2;
            const keys = ['snap', 'crackle', 'pop', 'foo', 'bar', 'baz'];
            let index = 0;

            // Ensure that the first promise won't resolve immediately. This
            // would block progress on a sequential scan but should pose no
            // problem for a parallel one.
            promiseFunc.mockImplementationOnce(() => new Promise(resolve => {
                    setTimeout(
                        resolve.bind(null, {
                            Items: [
                                {
                                    fizz: {S: 'quux'},
                                    bar: {NS: ['5', '12', '13']},
                                    baz: {L: [{BOOL: true}, {N: '101'}]},
                                },
                            ],
                        }),
                        50,
                    );
                }
            ));

            // Enqueue a number of responses that will resolve synchronously
            for (const key of keys) {
                promiseFunc.mockImplementationOnce(() => Promise.resolve({
                    Items: [
                        {
                            fizz: {S: key},
                            bar: {NS: [
                                    (++index).toString(10),
                                    (++index).toString(10),
                                ]},
                            baz: {L: [
                                    {BOOL: index % 2 === 0},
                                    {N: (++index).toString(10)}
                                ]},
                        },
                    ],
                    LastEvaluatedKey: {fizz: {S: key}},
                }));
            }

            // Enqueue a final page for this segment
            promiseFunc.mockImplementationOnce(() => Promise.resolve({Items: []}));

            const result: Array<any> = [];
            for await (const res of new ParallelScanIterator(
                mockDynamoDbClient as any,
                {
                    TableName: 'foo',
                    TotalSegments: segments,
                },
                ['fizz']
            )) {
                result.push(res);
            }

            expect(result).toEqual([
                {
                    fizz: {S: 'snap'},
                    bar: {NS: ['1', '2']},
                    baz: {L: [{BOOL: true}, {N: '3'}]}
                },
                {
                    fizz: {S: 'crackle'},
                    bar: {NS: ['4', '5']},
                    baz: {L: [{BOOL: false}, {N: '6'}]}
                },
                {
                    fizz: {S: 'pop'},
                    bar: {NS: ['7', '8']},
                    baz: {L: [{BOOL: true}, {N: '9'}]}
                },
                {
                    fizz: {S: 'foo'},
                    bar: {NS: ['10', '11']},
                    baz: {L: [{BOOL: false}, {N: '12'}]}
                },
                {
                    fizz: {S: 'bar'},
                    bar: {NS: ['13', '14']},
                    baz: {L: [{BOOL: true}, {N: '15'}]}
                },
                {
                    fizz: {S: 'baz'},
                    bar: {NS: ['16', '17']},
                    baz: {L: [{BOOL: false}, {N: '18'}]}
                },
                {
                    fizz: {S: 'quux'},
                    bar: {NS: ['5', '12', '13']},
                    baz: {L: [{BOOL: true}, {N: '101'}]}
                },
            ]);
        }
    );

    it('should provide access to the current scan state', async () => {
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
        promiseFunc.mockImplementationOnce(() => Promise.resolve({}));

        const iterator = new ParallelScanIterator(
            mockDynamoDbClient as any,
            {TableName: 'foo', TotalSegments: 2},
            ['fizz']
        );

        // each segment should be uninitialized before iteration starts
        expect(iterator.scanState).toEqual([
            {initialized: false},
            {initialized: false},
        ]);

        const expectedLastKeys = [
            {fizz: {S: 'snap'}},
            {fizz: {S: 'crackle'}},
            {fizz: {S: 'pop'}},
        ];

        for await (const _ of iterator) {
            expect(iterator.scanState).toEqual([
                {initialized: true, LastEvaluatedKey: expectedLastKeys.shift()},
                {initialized: false},
            ]);
        }

        expect(iterator.scanState).toEqual([
            {initialized: true},
            {initialized: true},
        ]);
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

        promiseFunc.mockImplementationOnce(() => Promise.resolve({}));

        const iterator = new ParallelScanIterator(
            mockDynamoDbClient as any,
            {TableName: 'foo', TotalSegments: 2},
            ['fizz']
        );

        for await (const _ of iterator) {
            // pass
        }

        expect(iterator.count).toBe(3);
        expect(iterator.scannedCount).toBe(6);
        expect(iterator.consumedCapacity).toEqual({
            TableName: 'foo',
            CapacityUnits: 6
        });
    });

    it(
        'should report the scan state even after ceasing iteration',
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

            const iterator = new ParallelScanIterator(
                mockDynamoDbClient as any,
                {TableName: 'foo', TotalSegments: 2},
                ['fizz']
            );

            for await (const _ of iterator) {
                break;
            }

            expect(iterator.scanState).toEqual([
                {initialized: true, LastEvaluatedKey: {fizz: {S: 'snap'}}},
                {initialized: false},
            ]);
        }
    );
});
