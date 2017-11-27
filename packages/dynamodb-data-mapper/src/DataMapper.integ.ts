import {DataMapper} from './DataMapper';
import {ItemNotFoundException} from './ItemNotFoundException';
import {DynamoDbSchema, DynamoDbTable} from './protocols';
import {hostname} from 'os';
import {hrtime} from 'process';
import DynamoDB = require('aws-sdk/clients/dynamodb');
import {DocumentType} from "@aws/dynamodb-data-marshaller";
import {Schema} from "@aws/dynamodb-data-marshaller";
import {equals} from "@aws/dynamodb-expressions";

const nestedDocumentDef: DocumentType = {
    type: 'Document',
    members: {
        foo: {type: 'String'}
    }
};
nestedDocumentDef.members.recursive = nestedDocumentDef;

interface NestedDocument {
    foo?: string;
    recursive?: NestedDocument;
}

const [seconds, nanoseconds] = hrtime();
const TableName = `mapper-integ-${seconds}-${nanoseconds}-${hostname()}`;
const schema: Schema = {
    key: {
        type: 'Number',
        attributeName: 'testIndex',
        keyType: 'HASH',
    },
    timestamp: {type: 'Date'},
    data: nestedDocumentDef,
    tuple: {
        type: 'Tuple',
        members: [
            {type: 'Boolean'},
            {type: 'String'},
        ]
    },
    scanIdentifier: {type: 'Number'}
};

class TestRecord {
    key: number;
    timestamp?: Date;
    data?: NestedDocument;
    tuple?: [boolean, string];
    scanIdentifier?: number;
}

Object.defineProperties(TestRecord.prototype, {
    [DynamoDbSchema]: {value: schema},
    [DynamoDbTable]: {value: TableName},
});

describe('DataMapper', () => {
    let idx = 0;
    const ddbClient = new DynamoDB();
    jest.setTimeout(60000);

    beforeAll(() => {
        return Promise.all([
            ddbClient.createTable({
                TableName,
                AttributeDefinitions: [
                    {
                        AttributeName: 'testIndex',
                        AttributeType: 'N',
                    }
                ],
                KeySchema: [
                    {
                        AttributeName: 'testIndex',
                        KeyType: 'HASH',
                    }
                ],
                ProvisionedThroughput: {
                    ReadCapacityUnits: 50,
                    WriteCapacityUnits: 50,
                },
            })
                .promise(),
            ddbClient.waitFor('tableExists', {TableName}).promise()
        ]);
    });

    afterAll(() => {
        return Promise.all([
            ddbClient.deleteTable({TableName}).promise(),
            ddbClient.waitFor('tableNotExists', {TableName}).promise()
        ]);
    });

    it('should save and load objects', async () => {
        const key = idx++;
        const mapper = new DataMapper({client: ddbClient});
        const timestamp = new Date();
        // subsecond precision will not survive the trip through the serializer,
        // as DynamoDB's ttl fields use unix epoch (second precision) timestamps
        timestamp.setMilliseconds(0);
        const item = new TestRecord();
        item.key = key;
        item.timestamp = timestamp;
        item.data = {
            recursive: {
                recursive: {
                    recursive: {
                        foo: '',
                    },
                },
            },
        };

        await mapper.put({item});

        expect(await mapper.get({item}))
            .toEqual(item);
    });

    it('should delete objects', async () => {
        const key = idx++;
        const mapper = new DataMapper({client: ddbClient});
        const timestamp = new Date();
        // subsecond precision will not survive the trip through the serializer,
        // as DynamoDB's ttl fields use unix epoch (second precision) timestamps
        timestamp.setMilliseconds(0);
        const item = new TestRecord();
        item.key = key;
        item.timestamp = timestamp;
        item.data = {
            recursive: {
                recursive: {
                    recursive: {
                        foo: '',
                    },
                },
            },
        };

        await mapper.put({item});

        await expect(mapper.get({item, readConsistency: 'strong'})).resolves;

        await mapper.delete({item});

        await expect(mapper.get({item, readConsistency: 'strong'}))
            .rejects
            .toMatchObject(new ItemNotFoundException({
                TableName,
                ConsistentRead: true,
                Key: {key: {N: key.toString(10)}}
            }));
    });

    it('should scan objects', async () => {
        const keys: Array<number> = [];
        const mapper = new DataMapper({client: ddbClient});
        const scanIdentifier = Date.now();

        const puts: Array<Promise<any>> = [];
        for (let i = 0; i < 10; i++) {
            const item = new TestRecord();
            item.key = idx++;
            item.tuple = [item.key % 2 === 0, 'string'];
            item.scanIdentifier = scanIdentifier;
            keys.push(item.key);
            puts.push(mapper.put({item}));
        }

        await Promise.all(puts);

        const results: Array<TestRecord> = [];
        for await (const element of mapper.scan({
            valueConstructor: TestRecord,
            readConsistency: 'strong',
            filter: {
                ...equals(scanIdentifier),
                subject: 'scanIdentifier'
            },
        })) {
            results.push(element);
        }

        expect(results.sort((a, b) => a.key - b.key)).toEqual(keys.map(key => {
            const record = new TestRecord();
            record.key = key;
            record.scanIdentifier = scanIdentifier;
            record.tuple = [key % 2 === 0, 'string'];
            return record;
        }));
    });

    it('should scan objects in parallel', async () => {
        const keys: Array<number> = [];
        const mapper = new DataMapper({client: ddbClient});
        const scanIdentifier = Date.now();

        const puts: Array<Promise<any>> = [];
        for (let i = 0; i < 10; i++) {
            const item = new TestRecord();
            item.key = idx++;
            item.tuple = [item.key % 2 === 0, 'string'];
            item.scanIdentifier = scanIdentifier;
            keys.push(item.key);
            puts.push(mapper.put({item}));
        }

        await Promise.all(puts);

        const results: Array<TestRecord> = [];
        for await (const element of mapper.parallelScan({
            valueConstructor: TestRecord,
            readConsistency: 'strong',
            segments: 4,
            filter: {
                ...equals(scanIdentifier),
                subject: 'scanIdentifier'
            },
        })) {
            results.push(element);
        }

        expect(results.sort((a, b) => a.key - b.key)).toEqual(keys.map(key => {
            const record = new TestRecord();
            record.key = key;
            record.scanIdentifier = scanIdentifier;
            record.tuple = [key % 2 === 0, 'string'];
            return record;
        }));
    });
});
