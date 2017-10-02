import {DataMapper} from './DataMapper';
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

    [DynamoDbSchema]() {
        return schema;
    }

    [DynamoDbTable]() {
        return TableName;
    }
}

describe('DataMapper', () => {
    let idx = 0;
    const ddbClient = new DynamoDB();
    jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000;

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
});
