import { BatchWrite } from './BatchWrite';
import { WriteRequest } from './types';

describe('BatchWrite', () => {
    const promiseFunc = jest.fn(() => Promise.resolve({
        UnprocessedItems: {}
    }));
    const mockDynamoDbClient = {
        config: {},
        batchWriteItem: jest.fn(() => ({promise: promiseFunc})),
    };

    beforeEach(() => {
        promiseFunc.mockClear();
        mockDynamoDbClient.batchWriteItem.mockClear();
    });

    it('should return itself when its Symbol.asyncIterator method is called', () => {
        const batchWrite = new BatchWrite({} as any, []);
        expect(batchWrite[Symbol.asyncIterator]()).toBe(batchWrite);
    });

    for (const asyncInput of [true, false]) {
        it(
            'should should partition write batches into requests with 25 or fewer items',
            async () => {
                const writes: Array<[string, WriteRequest]> = [];
                const expected: any = [
                    [
                        {
                            RequestItems: {
                                snap: [],
                                crackle: [],
                                pop: [],
                            }
                        }
                    ],
                    [
                        {
                            RequestItems: {
                                snap: [],
                                crackle: [],
                                pop: [],
                            }
                        }
                    ],
                    [
                        {
                            RequestItems: {
                                snap: [],
                                crackle: [],
                                pop: [],
                            }
                        }
                    ],
                    [
                        {
                            RequestItems: {
                                snap: [],
                                crackle: [],
                                pop: [],
                            }
                        }
                    ],
                ];

                for (let i = 0; i < 80; i++) {
                    const table = i % 3 === 0
                        ? 'snap'
                        : i % 3 === 1 ? 'crackle' : 'pop';
                    const fizz = { N: String(i) };
                    const req: WriteRequest = i % 2 === 0
                        ? {DeleteRequest: {Key: {fizz}}}
                        : {PutRequest: {Item: {fizz}}};
                    writes.push([table, req]);
                    expected[Math.floor(i / 25)][0].RequestItems[table]
                        .push(req);
                }

                const input = asyncInput
                    ? async function *() {
                        for (const item of writes) {
                            await new Promise(resolve => setTimeout(
                                resolve,
                                Math.round(Math.random())
                            ));
                            yield item;
                        }
                    }()
                    : writes;

                for await (const [tableName, req] of new BatchWrite(mockDynamoDbClient as any, input)) {
                    const id = req.DeleteRequest
                        ? parseInt(req.DeleteRequest.Key.fizz.N as string)
                        : parseInt((req.PutRequest as any).Item.fizz.N as string);

                    if (id % 3 === 0) {
                        expect(tableName).toBe('snap');
                    } else if (id % 3 === 1) {
                        expect(tableName).toBe('crackle');
                    } else {
                        expect(tableName).toBe('pop');
                    }
                }

                const {calls} = mockDynamoDbClient.batchWriteItem.mock;
                expect(calls.length).toBe(4);
                expect(calls).toEqual(expected);
            }
        );

        it('should should retry unprocessed items', async () => {
            const failures = new Set(['21', '24', '38', '43', '55', '60']);
            const writes: Array<[string, WriteRequest]> = [];
            const expected: any = [
                [
                    {
                        RequestItems: {
                            snap: [],
                            crackle: [],
                            pop: [],
                        }
                    }
                ],
                [
                    {
                        RequestItems: {
                            snap: [],
                            crackle: [],
                            pop: [],
                        }
                    }
                ],
                [
                    {
                        RequestItems: {
                            snap: [],
                            crackle: [],
                            pop: [],
                        }
                    }
                ],
                [
                    {
                        RequestItems: {
                            snap: [],
                            crackle: [],
                            pop: [],
                        }
                    }
                ],
            ];
            const responses: any = [
                { UnprocessedItems: {} },
                { UnprocessedItems: {} },
                { UnprocessedItems: {} },
                { UnprocessedItems: {} },
            ];

            for (let i = 0; i < 80; i++) {
                const table = i % 3 === 0
                    ? 'snap'
                    : i % 3 === 1 ? 'crackle' : 'pop';
                const fizz = { N: String(i) };
                const req: WriteRequest = i % 2 === 0
                    ? {DeleteRequest: {Key: {fizz}}}
                    : {PutRequest: {Item: {
                        fizz,
                        buzz: {B: new ArrayBuffer(3)},
                        pop: {B: Uint8Array.from([i])},
                        foo: {B: String.fromCharCode(i + 32)},
                        quux: {S: 'string'}
                    }}};
                writes.push([table, req]);
                expected[Math.floor(i / 25)][0].RequestItems[table]
                    .push(req);

                if (failures.has(fizz.N)) {
                    const {UnprocessedItems} = responses[Math.floor(i / 25)];
                    if (!(table in UnprocessedItems)) {
                        UnprocessedItems[table] = [];
                    }

                    UnprocessedItems[table].push(req);
                    expected[3][0].RequestItems[table].push(req);
                }
            }

            for (const response of responses) {
                promiseFunc.mockImplementationOnce(() => Promise.resolve(response));
            }

            const input = asyncInput
                ? async function *() {
                    for (const item of writes) {
                        await new Promise(resolve => setTimeout(
                            resolve,
                            Math.round(Math.random())
                        ));
                        yield item;
                    }
                }()
                : writes;

            for await (const [tableName, req] of new BatchWrite(mockDynamoDbClient as any, input)) {
                const id = req.DeleteRequest
                    ? parseInt(req.DeleteRequest.Key.fizz.N as string)
                    : parseInt((req.PutRequest as any).Item.fizz.N as string);

                if (id % 3 === 0) {
                    expect(tableName).toBe('snap');
                } else if (id % 3 === 1) {
                    expect(tableName).toBe('crackle');
                } else {
                    expect(tableName).toBe('pop');
                }
            }

            const {calls} = mockDynamoDbClient.batchWriteItem.mock;
            expect(calls.length).toBe(4);

            const callCount: {[key: string]: number} = calls.reduce(
                (
                    keyUseCount: {[key: string]: number},
                    [{RequestItems}]
                ) => {
                    for (const table of Object.keys(RequestItems)) {
                        for (const {PutRequest, DeleteRequest} of RequestItems[table]) {
                            let key = DeleteRequest
                                ? DeleteRequest.Key.fizz.N
                                : PutRequest.Item.fizz.N;
                            if (key in keyUseCount) {
                                keyUseCount[key]++;
                            } else {
                                keyUseCount[key] = 1;
                            }
                        }
                    }

                    return keyUseCount;
                },
                {}
            );

            for (let i = 0; i < 80; i++) {
                expect(callCount[i]).toBe(failures.has(String(i)) ? 2 : 1);
            }
        });
    }
});
