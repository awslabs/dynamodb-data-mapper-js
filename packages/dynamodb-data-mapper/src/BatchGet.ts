import { BatchOperation } from './BatchOperation';
import { PreparedElement } from './BatchTypes';
import {
    MAX_READ_BATCH_SIZE,
    ReadConsistency,
    StringToAnyObjectMap,
    SyncOrAsyncIterable,
} from './constants';
import { getKeyProperties } from './getKeyProperties';
import { itemIdentifier } from './itemIdentifier';
import { GetOptions } from './namedParameters';
import { getSchema } from './protocols';
import {
    marshallItem,
    toSchemaName,
    unmarshallItem,
    ZeroArgumentsConstructor,
} from '@aws/dynamodb-data-marshaller';
import {
    ExpressionAttributes,
    serializeProjectionExpression,
} from '@aws/dynamodb-expressions';
import {
    AttributeMap,
    BatchGetItemInput,
} from 'aws-sdk/clients/dynamodb';
import DynamoDB = require('aws-sdk/clients/dynamodb');

export class BatchGet<T extends StringToAnyObjectMap> extends
    BatchOperation<T, T, AttributeMap>
{
    protected readonly batchSize = MAX_READ_BATCH_SIZE;

    constructor(
        client: DynamoDB,
        items: SyncOrAsyncIterable<T>,
        private readonly defaultConsistency: ReadConsistency = 'eventual',
        tableNamePrefix: string = '',
        private readonly options: {[tableName: string]: GetOptions} = {}
    ) {
        super(client, items, tableNamePrefix);
    }

    protected async doBatchRequest() {
        const operationInput: BatchGetItemInput = {RequestItems: {}};
        let batchSize = 0;

        while (this.toSend.length > 0) {
            const [tableName, item] = this.toSend.shift() as [string, AttributeMap];
            if (operationInput.RequestItems[tableName] === undefined) {
                const {
                    projection,
                    readConsistency,
                    attributeNames,
                } = this.state[tableName];

                operationInput.RequestItems[tableName] = {
                    Keys: [],
                    ConsistentRead: readConsistency === 'strong',
                    ProjectionExpression: projection,
                    ExpressionAttributeNames: attributeNames,
                };
            }
            operationInput.RequestItems[tableName].Keys.push(item);

            if (++batchSize === this.batchSize) {
                break;
            }
        }

        const {
            Responses = {},
            UnprocessedKeys = {},
        } = await this.client.batchGetItem(operationInput).promise();

        const unprocessedTables = new Set<string>();
        for (const table of Object.keys(UnprocessedKeys)) {
            unprocessedTables.add(table);
            this.handleThrottled(table, UnprocessedKeys[table].Keys);
        }

        this.movePendingToThrottled(unprocessedTables);

        for (const table of Object.keys(Responses)) {
            const tableData = this.state[table];
            tableData.backoffFactor = Math.max(0, tableData.backoffFactor - 1);
            for (const item of Responses[table]) {
                const identifier = itemIdentifier(item, tableData.keyProperties);
                const {
                    constructor,
                    schema,
                } = tableData.itemConfigurations[identifier];
                this.pending.push(unmarshallItem<T>(schema, item, constructor));
            }
        }
    }

    protected prepareElement(item: T): PreparedElement<T, AttributeMap> {
        const schema = getSchema(item);
        const tableName = this.getTableName(item);
        if (!(tableName in this.state)) {
            const {
                projection,
                readConsistency = this.defaultConsistency,
            } = this.options[tableName] || {} as GetOptions;

            this.state[tableName] = {
                backoffFactor: 0,
                keyProperties: getKeyProperties(schema),
                name: tableName,
                readConsistency,
                itemConfigurations: {}
            };

            if (projection) {
                const attributes = new ExpressionAttributes();
                this.state[tableName].projection = serializeProjectionExpression(
                    projection.map(propName => toSchemaName(propName, schema)),
                    attributes
                );
                this.state[tableName].attributeNames = attributes.names;
            }
        }

        const tableState = this.state[tableName];
        const marshalled = marshallItem(schema, item);
        const identifier = itemIdentifier(marshalled, tableState.keyProperties);
        tableState.itemConfigurations[identifier] = {
            schema,
            constructor: item.constructor as ZeroArgumentsConstructor<T>,
        };

        return {
            marshalled,
            tableName,
            tableState,
        }
    }
}
