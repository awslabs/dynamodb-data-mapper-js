import {Repository} from "./Repository";
import {TableDefinition} from "@aws/dynamodb-data-marshaller";

describe('Repository', () => {
    const mockedMethods = [
        'delete',
        'get',
        'put',
        'query',
        'scan',
        'update',
    ];

    const mockDataMapper = mockedMethods.reduce(
        (prev, method) => {
            prev[method] = jest.fn();
            return prev;
        },
        {} as {[key: string]: jest.Mock<any>}
    );
    const tableDefinition: TableDefinition = {
        tableName: 'foo',
        schema: {},
    };

    const repository = new Repository({
        client: {} as any,
        tableDefinition,
    });
    (repository as any).dataMapper = mockDataMapper;

    beforeEach(() => {
        mockedMethods.forEach(method => {
            mockDataMapper[method].mockClear();
        });
    });

    for (const method of mockedMethods) {
        describe(`#${method}`, () => {
            it(`should call the ${method} method of the dataMapper`, () => {
                (repository as any)[method]({foo: 'bar'});

                expect(mockDataMapper[method].mock.calls.length).toBe(1);
                expect(mockDataMapper[method].mock.calls[0]).toEqual([
                    {
                        foo: 'bar',
                        tableDefinition,
                    },
                ]);
            });
        });
    }
});
