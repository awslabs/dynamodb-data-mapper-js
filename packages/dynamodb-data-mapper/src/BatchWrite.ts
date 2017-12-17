import { BatchOperation } from './BatchOperation';
import { WritePair, WriteType, PreparedElement } from './BatchTypes';
import {
    MAX_WRITE_BATCH_SIZE,
    StringToAnyObjectMap
} from './constants';
import { getKeyProperties } from './getKeyProperties';
import { itemIdentifier } from './itemIdentifier';
import { getSchema } from './protocols';
import {
    marshallKey,
    marshallItem,
    unmarshallItem,
    ZeroArgumentsConstructor,
} from '@aws/dynamodb-data-marshaller';
import {
    AttributeMap,
    BatchWriteItemInput
} from 'aws-sdk/clients/dynamodb';

export class BatchWrite<T extends StringToAnyObjectMap> extends
    BatchOperation<T, [WriteType, T], WritePair>
{
    readonly batchSize = MAX_WRITE_BATCH_SIZE;

    protected async doBatchRequest() {
        const putsInFlight: Array<[string, AttributeMap]> = [];
        const operationInput: BatchWriteItemInput = {RequestItems: {}};

        let batchSize = 0;
        while (this.toSend.length > 0) {
            const [
                tableName,
                [type, marshalled]
            ] = this.toSend.shift() as [string, WritePair];

            if (type === 'put') {
                putsInFlight.push([tableName, marshalled]);
            }

            if (operationInput.RequestItems[tableName] === undefined) {
                operationInput.RequestItems[tableName] = [];
            }
            operationInput.RequestItems[tableName].push(
                type === 'delete'
                    ? {DeleteRequest: {Key: marshalled}}
                    : {PutRequest: {Item: marshalled}}
            );

            if (++batchSize === this.batchSize) {
                break;
            }
        }

        const {UnprocessedItems = {}} = await this.client
            .batchWriteItem(operationInput).promise();
        const unprocessedTables = new Set<string>();
        const unprocessedPuts = new Set<string>();

        for (const table of Object.keys(UnprocessedItems)) {
            unprocessedTables.add(table);
            const unprocessed: Array<WritePair> = [];
            for (const item of UnprocessedItems[table]) {
                if (item.DeleteRequest) {
                    unprocessed.push(['delete', item.DeleteRequest.Key]);
                } else if (item.PutRequest) {
                    const marshalled = item.PutRequest.Item;
                    const { keyProperties } = this.state[table];
                    unprocessedPuts.add(
                        `${table}::${itemIdentifier(marshalled, keyProperties)}`
                    );
                    unprocessed.push(['put', marshalled]);
                }
            }

            this.handleThrottled(table, unprocessed);
        }

        this.movePendingToThrottled(unprocessedTables);

        const processedTables = new Set<string>();
        for (const [tableName, marshalled] of putsInFlight) {
            processedTables.add(tableName);
            const {keyProperties, itemConfigurations} = this.state[tableName];
            const identifier = itemIdentifier(marshalled, keyProperties);
            if (unprocessedPuts.has(`${tableName}::${identifier}`)) {
                continue;
            }

            const {
                constructor,
                schema,
            } = itemConfigurations[identifier];
            this.pending.push(
                unmarshallItem<T>(schema, marshalled, constructor)
            );
        }
    }

    protected prepareElement(
        [type, item]: [WriteType, T]
    ): PreparedElement<T, WritePair> {
        const schema = getSchema(item);
        const tableName = this.getTableName(item);
        if (!(tableName in this.state)) {
            this.state[tableName] = {
                backoffFactor: 0,
                keyProperties: getKeyProperties(schema),
                name: tableName,
                itemConfigurations: {}
            };
        }

        const marshalled = type === 'delete'
            ? marshallKey(schema, item)
            : marshallItem(schema, item);
        const tableState = this.state[tableName];
        const identifier = itemIdentifier(marshalled, tableState.keyProperties);
        tableState.itemConfigurations[identifier] = {
            schema,
            constructor: item.constructor as ZeroArgumentsConstructor<T>,

        };

        return {
            marshalled: [type, marshalled] as [WriteType, AttributeMap],
            tableName,
            tableState,
        }
    }
}
