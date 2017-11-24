import {DataMapper} from './DataMapper';
import {ItemNotFoundException} from './ItemNotFoundException';
import {DynamoDbSchema, DynamoDbTable} from './protocols';
import {hostname} from 'os';
import {hrtime} from 'process';
import DynamoDB = require('aws-sdk/clients/dynamodb');
import {DocumentType} from "@aws/dynamodb-data-marshaller";
import {Schema} from "@aws/dynamodb-data-marshaller";

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
    }
};

class TestRecord {
    key?: number;
    timestamp?: Date;
    data?: NestedDocument;
    tuple?: [boolean, string];
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
                    ReadCapacityUnits: 5,
                    WriteCapacityUnits: 5,
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
});
