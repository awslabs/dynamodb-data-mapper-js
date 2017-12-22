import { BatchGet } from './BatchGet';
import { AttributeMap } from 'aws-sdk/clients/dynamodb';

describe('BatchGet', () => {
    const promiseFunc = jest.fn(() => Promise.resolve({
        UnprocessedItems: {}
    }));
    const mockDynamoDbClient = {
        config: {},
        batchGetItem: jest.fn(() => ({promise: promiseFunc})),
    };

    beforeEach(() => {
        promiseFunc.mockClear();
        mockDynamoDbClient.batchGetItem.mockClear();
    });

    it('should return itself when its Symbol.asyncIterator method is called', () => {
        const batchGet = new BatchGet({} as any, []);
        expect(batchGet[Symbol.asyncIterator]()).toBe(batchGet);
    });

    it('should allow setting an overall read consistency', async () => {
        const batchGet = new BatchGet(
            mockDynamoDbClient as any,
            [['foo', {fizz: {N: '0'}}]],
            {ConsistentRead: true}
        );
        for await (const _ of batchGet) {
            console.log(_ === undefined);
            // pass
        }

        expect(mockDynamoDbClient.batchGetItem.mock.calls).toEqual([
            [
                {
                    RequestItems: {
                        foo: {
                            Keys: [
                                {fizz: {N: '0'}}
                            ],
                            ConsistentRead: true
                        }
                    }
                }
            ]
        ])
    });

    it('should allow setting per-table read consistency', async () => {
        const batchGet = new BatchGet(
            mockDynamoDbClient as any,
            [
                ['foo', {fizz: {N: '0'}}],
                ['bar', {quux: {N: '1'}}],
            ],
            {
                ConsistentRead: true,
                PerTableOptions: {
                    bar: { ConsistentRead: false }
                }
            }
        );

        for await (const _ of batchGet) {
            // pass
        }

        expect(mockDynamoDbClient.batchGetItem.mock.calls).toEqual([
            [
                {
                    RequestItems: {
                        foo: {
                            Keys: [
                                {fizz: {N: '0'}}
                            ],
                            ConsistentRead: true
                        },
                        bar: {
                            Keys: [
                                {quux: {N: '1'}}
                            ],
                            ConsistentRead: false
                        }
                    }
                }
            ]
        ]);
    });

    it('should allow specifying per-table projection expressions', async () => {
        const batchGet = new BatchGet(
            mockDynamoDbClient as any,
            [
                ['foo', {fizz: {N: '0'}}],
                ['bar', {quux: {N: '1'}}],
            ],
            {
                PerTableOptions: {
                    bar: {
                        ProjectionExpression: 'snap[1].crackle.pop[2]'
                    }
                }
            }
        );

        for await (const _ of batchGet) {
            // pass
        }

        expect(mockDynamoDbClient.batchGetItem.mock.calls).toEqual([
            [
                {
                    RequestItems: {
                        foo: {
                            Keys: [
                                {fizz: {N: '0'}}
                            ]
                        },
                        bar: {
                            Keys: [
                                {quux: {N: '1'}}
                            ],
                            ProjectionExpression: 'snap[1].crackle.pop[2]'
                        }
                    }
                }
            ]
        ]);
    });

    for (const asyncInput of [true, false]) {
        it(
            'should should partition get batches into requests with 100 or fewer items',
            async () => {
                const gets: Array<[string, AttributeMap]> = [];
                const expected: any = [
                    [
                        {
                            RequestItems: {
                                snap: { Keys: [] },
                                crackle: { Keys: [] },
                                pop: { Keys: [] },
                            }
                        }
                    ],
                    [
                        {
                            RequestItems: {
                                snap: { Keys: [] },
                                crackle: { Keys: [] },
                                pop: { Keys: [] },
                            }
                        }
                    ],
                    [
                        {
                            RequestItems: {
                                snap: { Keys: [] },
                                crackle: { Keys: [] },
                                pop: { Keys: [] },
                            }
                        }
                    ],
                    [
                        {
                            RequestItems: {
                                snap: { Keys: [] },
                                crackle: { Keys: [] },
                                pop: { Keys: [] },
                            }
                        }
                    ],
                ];
                const responses: any = [
                    {
                        Responses: {
                            snap: [],
                            crackle: [],
                            pop: [],
                        }
                    },
                    {
                        Responses: {
                            snap: [],
                            crackle: [],
                            pop: [],
                        }
                    },
                    {
                        Responses: {
                            snap: [],
                            crackle: [],
                            pop: [],
                        }
                    },
                    {
                        Responses: {
                            snap: [],
                            crackle: [],
                            pop: [],
                        }
                    },
                ];

                for (let i = 0; i < 325; i++) {
                    const table = i % 3 === 0
                        ? 'snap'
                        : i % 3 === 1 ? 'crackle' : 'pop';
                    const fizz = { N: String(i) };
                    const buzz = { S: 'Static string' };
                    gets.push([table, {fizz: {N: String(i)}}]);

                    responses[Math.floor(i / 100)].Responses[table]
                        .push({fizz, buzz});
                    expected[Math.floor(i / 100)][0].RequestItems[table].Keys
                        .push({fizz});
                }

                for (const response of responses) {
                    promiseFunc.mockImplementationOnce(
                        () => Promise.resolve(response)
                    );
                }

                const input = asyncInput
                    ? async function *() {
                        for (const item of gets) {
                            await new Promise(resolve => setTimeout(
                                resolve,
                                Math.round(Math.random())
                            ));
                            yield item;
                        }
                    }()
                    : gets;

                for await (const [table, item] of new BatchGet(mockDynamoDbClient as any, input)) {
                    const id = parseInt(item.fizz.N as string);
                    if (id % 3 === 0) {
                        expect(table).toBe('snap');
                    } else if (id % 3 === 1) {
                        expect(table).toBe('crackle');
                    } else {
                        expect(table).toBe('pop');
                    }

                    expect(item.buzz).toEqual({ S: 'Static string' });
                }

                const {calls} = mockDynamoDbClient.batchGetItem.mock;
                expect(calls.length).toBe(4);
                expect(calls).toEqual(expected);
            }
        );

        it('should should retry unprocessed items', async () => {
            const failures = new Set(['24', '142', '260']);

            const gets: Array<[string, AttributeMap]> = [];
            const expected: any = [
                [
                    {
                        RequestItems: {
                            snap: { Keys: [] },
                            crackle: { Keys: [] },
                            pop: { Keys: [] },
                        }
                    }
                ],
                [
                    {
                        RequestItems: {
                            snap: { Keys: [] },
                            crackle: { Keys: [] },
                            pop: { Keys: [] },
                        }
                    }
                ],
                [
                    {
                        RequestItems: {
                            snap: { Keys: [] },
                            crackle: { Keys: [] },
                            pop: { Keys: [] },
                        }
                    }
                ],
                [
                    {
                        RequestItems: {
                            snap: { Keys: [] },
                            crackle: { Keys: [] },
                            pop: { Keys: [] },
                        }
                    }
                ],
            ];
            const responses: any = [
                {
                    Responses: {
                        snap: [],
                        crackle: [],
                        pop: [],
                    }
                },
                {
                    Responses: {
                        snap: [],
                        crackle: [],
                        pop: [],
                    }
                },
                {
                    Responses: {
                        snap: [],
                        crackle: [],
                        pop: [],
                    }
                },
                {
                    Responses: {
                        snap: [],
                        crackle: [],
                        pop: [],
                    }
                },
            ];

            for (let i = 0; i < 325; i++) {
                const table = i % 3 === 0
                    ? 'snap'
                    : i % 3 === 1 ? 'crackle' : 'pop';
                const fizz = { N: String(i) };
                const buzz = { S: 'Static string' };
                gets.push([table, {fizz: {N: String(i)}}]);

                expected[Math.floor(i / 100)][0].RequestItems[table].Keys
                    .push({fizz});

                if (failures.has(String(i))) {
                    responses[Math.floor(i / 100)].UnprocessedKeys = {
                        [table]: { Keys: [{fizz}] }
                    };
                    responses[3].Responses[table]
                        .push({fizz, buzz});
                } else {
                    responses[Math.floor(i / 100)].Responses[table]
                        .push({fizz, buzz});
                }
            }

            for (const response of responses) {
                promiseFunc.mockImplementationOnce(
                    () => Promise.resolve(response)
                );
            }

            const input = asyncInput
                ? async function *() {
                    for (const item of gets) {
                        await new Promise(resolve => setTimeout(
                            resolve,
                            Math.round(Math.random())
                        ));
                        yield item;
                    }
                }()
                : gets;

            let idsReturned = new Set<number>();
            for await (const [table, item] of new BatchGet(mockDynamoDbClient as any, input)) {
                const id = parseInt(item.fizz.N as string);
                try {
                    expect(idsReturned.has(id)).toBe(false);
                } catch (err) {
                    console.log(id);
                }
                idsReturned.add(id);

                if (id % 3 === 0) {
                    expect(table).toBe('snap');
                } else if (id % 3 === 1) {
                    expect(table).toBe('crackle');
                } else {
                    expect(table).toBe('pop');
                }

                expect(item.buzz).toEqual({ S: 'Static string' });
            }

            expect(idsReturned.size).toBe(325);

            const {calls} = mockDynamoDbClient.batchGetItem.mock;
            expect(calls.length).toBe(4);

            const callCount: {[key: string]: number} = calls.reduce(
                (
                    keyUseCount: {[key: string]: number},
                    [{RequestItems}]
                ) => {
                    const keys = [];
                    for (const table of Object.keys(RequestItems)) {
                        keys.push(...RequestItems[table].Keys);
                    }
                    for (const {fizz: {N: key}} of keys) {
                        if (key in keyUseCount) {
                            keyUseCount[key]++;
                        } else {
                            keyUseCount[key] = 1;
                        }
                    }

                    return keyUseCount;
                },
                {}
            );

            for (let i = 0; i < 325; i++) {
                expect(callCount[i]).toBe(failures.has(String(i)) ? 2 : 1);
            }
        });
    }
});
