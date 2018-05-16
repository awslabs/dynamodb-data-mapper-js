import { BatchState } from './BatchState';
import {
    ReadConsistency,
    StringToAnyObjectMap,
    SyncOrAsyncIterable,
    VERSION,
    WriteType,
} from './constants';
import { ItemNotFoundException } from "./ItemNotFoundException";
import {
    BaseScanOptions,
    BatchGetOptions,
    BatchGetTableOptions,
    CreateTableOptions,
    DataMapperConfiguration,
    DeleteOptions,
    DeleteParameters,
    GetOptions,
    GetParameters,
    ParallelScanParameters,
    ParallelScanWorkerOptions,
    ParallelScanWorkerParameters,
    PerIndexOptions,
    PutOptions,
    PutParameters,
    QueryOptions,
    QueryParameters,
    ScanOptions,
    ScanParameters,
    SecondaryIndexProjection,
    UpdateOptions,
    UpdateParameters,
} from './namedParameters';
import {
    DynamoDbTable,
    getSchema,
    getTableName,
} from './protocols';
import {
    BatchGet,
    BatchWrite,
    PerTableOptions,
    TableOptions,
    WriteRequest,
} from '@aws/dynamodb-batch-iterator';
import {
    AttributeTypeMap,
    getSchemaName,
    isKey,
    keysFromSchema,
    KeyTypeMap,
    marshallItem,
    marshallKey,
    marshallValue,
    PerIndexKeys,
    Schema,
    SchemaType,
    toSchemaName,
    unmarshallItem,
    ZeroArgumentsConstructor,
} from "@aws/dynamodb-data-marshaller";
import {
    AttributePath,
    AttributeValue,
    ConditionExpression,
    ConditionExpressionPredicate,
    ExpressionAttributes,
    FunctionExpression,
    isConditionExpression,
    isConditionExpressionPredicate,
    MathematicalExpression,
    PathElement,
    serializeConditionExpression,
    serializeProjectionExpression,
    UpdateExpression,
} from "@aws/dynamodb-expressions";
import {
    AttributeDefinition,
    AttributeMap,
    DeleteItemInput,
    GetItemInput,
    GlobalSecondaryIndexList,
    KeySchemaElement,
    LocalSecondaryIndexList,
    Projection,
    PutItemInput,
    QueryInput,
    QueryOutput,
    ScanInput,
    ScanOutput,
    UpdateItemInput,
} from "aws-sdk/clients/dynamodb";
import DynamoDB = require('aws-sdk/clients/dynamodb');

require('./asyncIteratorSymbolPolyfill');

/**
 * Object mapper for domain object interaction with DynamoDB.
 *
 * To use, define a schema that describes how an item is represented in a
 * DynamoDB table. This schema will be used to marshall a native JavaScript
 * object into its desired persisted form. Attributes present on the object
 * but not in the schema will be ignored.
 */
export class DataMapper {
    private readonly client: DynamoDB;
    private readonly readConsistency: ReadConsistency;
    private readonly skipVersionCheck: boolean;
    private readonly tableNamePrefix: string;

    constructor({
        client,
        readConsistency = 'eventual',
        skipVersionCheck = false,
        tableNamePrefix = ''
    }: DataMapperConfiguration) {
        client.config.customUserAgent = ` dynamodb-data-mapper-js/${VERSION}`;
        this.client = client;
        this.readConsistency = readConsistency;
        this.skipVersionCheck = skipVersionCheck;
        this.tableNamePrefix = tableNamePrefix;
    }

    /**
     * Deletes items from DynamoDB in batches of 25 or fewer via one or more
     * BatchWriteItem operations. The items may be from any number of tables;
     * tables and schemas for each item are determined using the
     * {DynamoDbSchema} property and the {DynamoDbTable} property on defined on
     * each item supplied.
     *
     * This method will automatically retry any delete requests returned by
     * DynamoDB as unprocessed. Exponential backoff on unprocessed items is
     * employed on a per-table basis.
     *
     * @param items A synchronous or asynchronous iterable of items to delete.
     */
    async *batchDelete<T extends StringToAnyObjectMap>(
        items: SyncOrAsyncIterable<T>
    ) {
        const iter = this.batchWrite(
            async function *mapToDelete(): AsyncIterable<['delete', T]> {
                for await (const item of items) {
                    yield ['delete', item];
                }
            }()
        );

        for await (const [_, unmarshalled] of iter) {
            yield unmarshalled;
        }
    }

    /**
     * Retrieves items from DynamoDB in batches of 100 or fewer via one or more
     * BatchGetItem operations. The items may be from any number of tables;
     * tables and schemas for each item are determined using the
     * {DynamoDbSchema} property and the {DynamoDbTable} property on defined on
     * each item supplied.
     *
     * This method will automatically retry any get requests returned by
     * DynamoDB as unprocessed. Exponential backoff on unprocessed items is
     * employed on a per-table basis.
     *
     * @param items A synchronous or asynchronous iterable of items to get.
     */
    async *batchGet<T extends StringToAnyObjectMap>(
        items: SyncOrAsyncIterable<T>,
        {
            readConsistency = this.readConsistency,
            perTableOptions = {}
        }: BatchGetOptions = {}
    ) {
        const state: BatchState<T> = {};
        const options: PerTableOptions = {};

        const batch = new BatchGet(
            this.client,
            this.mapGetBatch(items, state, perTableOptions, options),
            {
                ConsistentRead: readConsistency === 'strong',
                PerTableOptions: options
            }
        );

        for await (const [tableName, marshalled] of batch) {
            const {keyProperties, itemSchemata} = state[tableName];
            const {
                constructor,
                schema,
            } = itemSchemata[itemIdentifier(marshalled, keyProperties)];
            yield unmarshallItem<T>(schema, marshalled, constructor);
        }
    }

    /**
     * Puts items into DynamoDB in batches of 25 or fewer via one or more
     * BatchWriteItem operations. The items may be from any number of tables;
     * tables and schemas for each item are determined using the
     * {DynamoDbSchema} property and the {DynamoDbTable} property on defined on
     * each item supplied.
     *
     * This method will automatically retry any put requests returned by
     * DynamoDB as unprocessed. Exponential backoff on unprocessed items is
     * employed on a per-table basis.
     *
     * @param items A synchronous or asynchronous iterable of items to put.
     */
    async *batchPut<T extends StringToAnyObjectMap>(
        items: SyncOrAsyncIterable<T>
    ) {
        const generator: SyncOrAsyncIterable<[WriteType, T]> = isIterable(items)
            ? function *mapToPut() {
                for (const item of items) {
                    yield ['put', item] as [WriteType, T];
                }
            }()
            : async function *mapToPut() {
                for await (const item of items) {
                    yield ['put', item] as [WriteType, T];
                }
            }();

        for await (const [_, unmarshalled] of this.batchWrite(generator)) {
            yield unmarshalled;
        }
    }

    /**
     * Puts or deletes items from DynamoDB in batches of 25 or fewer via one or
     * more BatchWriteItem operations. The items may belong to any number of
     * tables; tables and schemas for each item are determined using the
     * {DynamoDbSchema} property and the {DynamoDbTable} property on defined on
     * each item supplied.
     *
     * This method will automatically retry any write requests returned by
     * DynamoDB as unprocessed. Exponential backoff on unprocessed items is
     * employed on a per-table basis.
     *
     * @param items A synchronous or asynchronous iterable of tuples of the
     * string 'put'|'delete' and the item on which to perform the specified
     * write action.
     */
    async *batchWrite<T extends StringToAnyObjectMap>(
        items: SyncOrAsyncIterable<[WriteType, T]>
    ): AsyncIterableIterator<[WriteType, T]> {
        const state: BatchState<T> = {};
        const batch = new BatchWrite(
            this.client,
            this.mapWriteBatch(items, state)
        );

        for await (const [tableName, {DeleteRequest, PutRequest}] of batch) {
            const {keyProperties, itemSchemata} = state[tableName];
            const attributes = PutRequest
                ? PutRequest.Item
                : (DeleteRequest || {Key: {}}).Key
            const {
                constructor,
                schema,
            } = itemSchemata[itemIdentifier(attributes, keyProperties)];

            yield [
                PutRequest ? 'put' : 'delete',
                unmarshallItem<T>(schema, attributes, constructor)
            ];
        }
    }

    /**
     * Perform a CreateTable operation using the schema accessible via the
     * {DynamoDbSchema} property and the table name accessible via the
     * {DynamoDbTable} property on the prototype of the constructor supplied.
     *
     * The promise returned by this method will not resolve until the table is
     * active and ready for use.
     *
     * @param valueConstructor  The constructor used for values in the table.
     * @param options           Options to configure the CreateTable operation
     */
    async createTable(
        valueConstructor: ZeroArgumentsConstructor<any>,
        {
            readCapacityUnits,
            streamViewType = 'NONE',
            writeCapacityUnits,
            indexOptions = {},
        }: CreateTableOptions
    ) {
        const schema = getSchema(valueConstructor.prototype);
        const { attributes, indexKeys, tableKeys } = keysFromSchema(schema);
        const TableName = this.getTableName(valueConstructor.prototype);

        const {
            TableDescription: {TableStatus} = {TableStatus: 'CREATING'}
        } = await this.client.createTable({
            ...indexDefinitions(indexKeys, indexOptions, schema),
            TableName,
            ProvisionedThroughput: {
                ReadCapacityUnits: readCapacityUnits,
                WriteCapacityUnits: writeCapacityUnits,
            },
            AttributeDefinitions: attributeDefinitionList(attributes),
            KeySchema: keyTypesToElementList(tableKeys),
            StreamSpecification: streamViewType === 'NONE'
                ? { StreamEnabled: false }
                : { StreamEnabled: true, StreamViewType: streamViewType },
        }).promise();

        if (TableStatus !== 'ACTIVE') {
            await this.client.waitFor('tableExists', {TableName}).promise();
        }
    }

    /**
     * Perform a DeleteItem operation using the schema accessible via the
     * {DynamoDbSchema} property and the table name accessible via the
     * {DynamoDbTable} property on the item supplied.
     *
     * @param item      The item to delete
     * @param options   Options to configure the DeleteItem operation
     */
    delete<T extends StringToAnyObjectMap = StringToAnyObjectMap>(
        item: T,
        options?: DeleteOptions
    ): Promise<T|undefined>;

    /**
     * @deprecated
     */
    delete<T extends StringToAnyObjectMap = StringToAnyObjectMap>(
        parameters: DeleteParameters<T>
    ): Promise<T|undefined>;

    async delete<T extends StringToAnyObjectMap = StringToAnyObjectMap>(
        itemOrParameters: T|DeleteParameters<T>,
        options: DeleteOptions = {}
    ): Promise<T|undefined> {
        let item: T;
        if (
            'item' in itemOrParameters &&
            (itemOrParameters as DeleteParameters<T>).item[DynamoDbTable]
        ) {
            item = (itemOrParameters as DeleteParameters<T>).item;
            options = itemOrParameters as DeleteParameters<T>;
        } else {
            item = itemOrParameters as T;
        }
        let {
            condition,
            returnValues = 'ALL_OLD',
            skipVersionCheck = this.skipVersionCheck,
        } = options;

        const schema = getSchema(item);

        const req: DeleteItemInput = {
            TableName: this.getTableName(item),
            Key: marshallKey(schema, item),
            ReturnValues: returnValues,
        };

        if (!skipVersionCheck) {
            for (const prop of Object.keys(schema)) {
                let inputMember = item[prop];
                const {attributeName = prop, ...fieldSchema} = schema[prop];

                if (isVersionAttribute(fieldSchema) && inputMember !== undefined) {
                    const {condition: versionCondition} = handleVersionAttribute(
                        attributeName,
                        inputMember
                    );

                    condition = condition
                        ? {type: 'And', conditions: [condition, versionCondition]}
                        : versionCondition;
                }
            }
        }

        if (condition) {
            const attributes = new ExpressionAttributes();
            req.ConditionExpression = serializeConditionExpression(
                normalizeConditionExpressionPaths(condition, schema),
                attributes
            );

            if (Object.keys(attributes.names).length > 0) {
                req.ExpressionAttributeNames = attributes.names;
            }

            if (Object.keys(attributes.values).length > 0) {
                req.ExpressionAttributeValues = attributes.values;
            }
        }

        const {Attributes} = await this.client.deleteItem(req).promise();
        if (Attributes) {
            return unmarshallItem<T>(
                schema,
                Attributes,
                item.constructor as ZeroArgumentsConstructor<T>
            );
        }
    }

    /**
     * Perform a DeleteTable operation using the schema accessible via the
     * {DynamoDbSchema} property and the table name accessible via the
     * {DynamoDbTable} property on the prototype of the constructor supplied.
     *
     * The promise returned by this method will not resolve until the table is
     * deleted and can no longer be used.
     *
     * @param valueConstructor  The constructor used for values in the table.
     */
    async deleteTable(valueConstructor: ZeroArgumentsConstructor<any>) {
        const TableName = this.getTableName(valueConstructor.prototype);
        await this.client.deleteTable({TableName}).promise();
        await this.client.waitFor('tableNotExists', {TableName}).promise();
    }

    /**
     * If the table does not already exist, perform a CreateTable operation
     * using the schema accessible via the {DynamoDbSchema} property and the
     * table name accessible via the {DynamoDbTable} property on the prototype
     * of the constructor supplied.
     *
     * The promise returned by this method will not resolve until the table is
     * active and ready for use.
     *
     * @param valueConstructor  The constructor used for values in the table.
     * @param options           Options to configure the CreateTable operation
     */
    async ensureTableExists(
        valueConstructor: ZeroArgumentsConstructor<any>,
        options: CreateTableOptions
    ) {
        const TableName = this.getTableName(valueConstructor.prototype);
        try {
            const {
                Table: {TableStatus} = {TableStatus: 'CREATING'}
            } = await this.client.describeTable({TableName}).promise();

            if (TableStatus !== 'ACTIVE') {
                await this.client.waitFor('tableExists', {TableName}).promise();
            }
        } catch (err) {
            if (err.name === 'ResourceNotFoundException') {
                await this.createTable(valueConstructor, options);
            } else {
                throw err;
            }
        }
    }

    /**
     * If the table exists, perform a DeleteTable operation using the schema
     * accessible via the {DynamoDbSchema} property and the table name
     * accessible via the {DynamoDbTable} property on the prototype of the
     * constructor supplied.
     *
     * The promise returned by this method will not resolve until the table is
     * deleted and can no longer be used.
     *
     * @param valueConstructor  The constructor used for values in the table.
     */
    async ensureTableNotExists(
        valueConstructor: ZeroArgumentsConstructor<any>
    ) {
        const TableName = this.getTableName(valueConstructor.prototype);
        try {
            const {
                Table: {TableStatus: status} = {TableStatus: 'CREATING'}
            } = await this.client.describeTable({TableName}).promise();

            if (status === 'DELETING') {
                await this.client.waitFor('tableNotExists', {TableName})
                    .promise();
                return;
            } else if (status === 'CREATING' || status === 'UPDATING') {
                await this.client.waitFor('tableExists', {TableName})
                    .promise();
            }

            await this.deleteTable(valueConstructor);
        } catch (err) {
            if (err.name !== 'ResourceNotFoundException') {
                throw err;
            }
        }
    }

    /**
     * Perform a GetItem operation using the schema accessible via the
     * {DynamoDbSchema} method and the table name accessible via the
     * {DynamoDbTable} method on the item supplied.
     *
     * @param item      The item to get
     * @param options   Options to configure the GetItem operation
     */
    get<T extends StringToAnyObjectMap = StringToAnyObjectMap>(
        item: T,
        options?: GetOptions
    ): Promise<T>;

    /**
     * @deprecated
     */
    get<T extends StringToAnyObjectMap = StringToAnyObjectMap>(
        parameters: GetParameters<T>
    ): Promise<T>;

    async get<T extends StringToAnyObjectMap = StringToAnyObjectMap>(
        itemOrParameters: T|GetParameters<T>,
        options: GetOptions = {}
    ): Promise<T|undefined> {
        let item: T;
        if (
            'item' in itemOrParameters &&
            (itemOrParameters as GetParameters<T>).item[DynamoDbTable]
        ) {
            item = (itemOrParameters as GetParameters<T>).item;
            options = itemOrParameters as GetParameters<T>;
        } else {
            item = itemOrParameters as T;
        }
        const {
            projection,
            readConsistency = this.readConsistency
        } = options;

        const schema = getSchema(item);
        const req: GetItemInput = {
            TableName: this.getTableName(item),
            Key: marshallKey(schema, item),
            ConsistentRead: readConsistency === 'strong',
        };

        if (projection) {
            const attributes = new ExpressionAttributes();
            req.ProjectionExpression = serializeProjectionExpression(
                projection.map(propName => toSchemaName(propName, schema)),
                attributes
            );

            if (Object.keys(attributes.names).length > 0) {
                req.ExpressionAttributeNames = attributes.names;
            }
        }

        const {Item} = await this.client.getItem(req).promise();
        if (Item) {
            return unmarshallItem<T>(
                schema,
                Item,
                item.constructor as ZeroArgumentsConstructor<T>
            );
        }

        throw new ItemNotFoundException(req);
    }

    /**
     * Perform a Scan operation using the schema accessible via the
     * {DynamoDbSchema} method and the table name accessible via the
     * {DynamoDbTable} method on the prototype of the constructor supplied.
     *
     * This scan will be performed by multiple parallel workers, each of which
     * will perform a sequential scan of a segment of the table or index. Use
     * the `segments` parameter to specify the number of workers to be used.
     *
     * @param valueConstructor  The constructor to be used for each item
     *                          returned by the scan
     * @param segments          The number of parallel workers to use to perform
     *                          the scan
     * @param options           Options to configure the Scan operation
     *
     * @return An asynchronous iterator that yields scan results. Intended
     * to be consumed with a `for await ... of` loop.
     */
    parallelScan<T extends StringToAnyObjectMap>(
        valueConstructor: ZeroArgumentsConstructor<T>,
        segments: number,
        options?: BaseScanOptions
    ): AsyncIterableIterator<T>;

    /**
     * @deprecated
     */
    parallelScan<T extends StringToAnyObjectMap>(
        parameters: ParallelScanParameters<T>
    ): AsyncIterableIterator<T>;

    async *parallelScan<T extends StringToAnyObjectMap>(
        ctorOrParams: ZeroArgumentsConstructor<T>|ParallelScanParameters<T>,
        segments?: number,
        options: BaseScanOptions = {}
    ): AsyncIterableIterator<T> {
        let valueConstructor: ZeroArgumentsConstructor<T>;
        if (typeof segments !== 'number') {
            valueConstructor = (ctorOrParams as ParallelScanParameters<T>).valueConstructor;
            segments = (ctorOrParams as ParallelScanParameters<T>).segments;
            options = ctorOrParams as ParallelScanParameters<T>;
        } else {
            valueConstructor = ctorOrParams as ZeroArgumentsConstructor<T>;
        }

        const req = this.buildScanInput(valueConstructor, options);
        const schema = getSchema(valueConstructor.prototype);

        interface PendingResult {
            iterator: AsyncIterator<T>;
            result: Promise<{
                iterator: AsyncIterator<T>;
                result: IteratorResult<T>
            }>;
        }

        const pendingResults: Array<PendingResult> = [];
        function addToPending(iterator: AsyncIterator<T>): void {
            const result = iterator.next().then(resolved => ({
                iterator,
                result: resolved
            }));
            pendingResults.push({iterator, result});
        }

        for (let i = 0; i < segments; i++) {
            addToPending(this.doSequentialScan(
                {
                    ...req,
                    TotalSegments: segments,
                    Segment: i
                },
                schema,
                valueConstructor
            ));
        }

        while (pendingResults.length > 0) {
            const {
                result: {value, done},
                iterator
            } = await Promise.race(pendingResults.map(val => val.result));

            for (let i = pendingResults.length - 1; i >= 0; i--) {
                if (pendingResults[i].iterator === iterator) {
                    pendingResults.splice(i, 1);
                }
            }

            if (!done) {
                addToPending(iterator);
                yield value;
            }
        }
    }

    /**
     * Perform a PutItem operation using the schema accessible via the
     * {DynamoDbSchema} method and the table name accessible via the
     * {DynamoDbTable} method on the item supplied.
     *
     * @param item      The item to save to DynamoDB
     * @param options   Options to configure the PutItem operation
     */
    put<T extends StringToAnyObjectMap = StringToAnyObjectMap>(
        item: T,
        options?: PutOptions
    ): Promise<T>;

    /**
     * @deprecated
     */
    put<T extends StringToAnyObjectMap = StringToAnyObjectMap>(
        parameters: PutParameters<T>
    ): Promise<T>;

    async put<T extends StringToAnyObjectMap = StringToAnyObjectMap>(
        itemOrParameters: T|PutParameters<T>,
        options: PutOptions = {}
    ): Promise<T> {
        let item: T;
        if (
            'item' in itemOrParameters &&
            (itemOrParameters as PutParameters<T>).item[DynamoDbTable]
        ) {
            item = (itemOrParameters as PutParameters<T>).item;
            options = itemOrParameters as PutParameters<T>;
        } else {
            item = itemOrParameters as T;
        }
        let {
            condition,
            skipVersionCheck = this.skipVersionCheck,
        } = options;

        const schema = getSchema(item);
        const req: PutItemInput = {
            TableName: this.getTableName(item),
            Item: marshallItem(schema, item),
        };

        if (!skipVersionCheck) {
            for (const key of Object.keys(schema)) {
                let inputMember = item[key];
                const fieldSchema = schema[key];
                const {attributeName = key} = fieldSchema;

                if (isVersionAttribute(fieldSchema)) {
                    const {condition: versionCond} = handleVersionAttribute(
                        attributeName,
                        inputMember
                    );
                    if (req.Item[attributeName]) {
                        req.Item[attributeName].N = (
                            Number(req.Item[attributeName].N) + 1
                        ).toString();
                    } else {
                        req.Item[attributeName] = {N: "0"};
                    }

                    condition = condition
                        ? {type: 'And', conditions: [condition, versionCond]}
                        : versionCond;
                }
            }
        }

        if (condition) {
            const attributes = new ExpressionAttributes();
            req.ConditionExpression = serializeConditionExpression(
                normalizeConditionExpressionPaths(condition, schema),
                attributes
            );

            if (Object.keys(attributes.names).length > 0) {
                req.ExpressionAttributeNames = attributes.names;
            }

            if (Object.keys(attributes.values).length > 0) {
                req.ExpressionAttributeValues = attributes.values;
            }
        }

        await this.client.putItem(req).promise();

        return unmarshallItem<T>(
            schema,
            req.Item,
            item.constructor as ZeroArgumentsConstructor<T>
        );
    }

    /**
     * Perform a Query operation using the schema accessible via the
     * {DynamoDbSchema} method and the table name accessible via the
     * {DynamoDbTable} method on the prototype of the constructor supplied.
     *
     * @param valueConstructor  The constructor to use for each query result.
     * @param keyCondition      A condition identifying a particular hash key
     *                          value.
     * @param options           Additional options for customizing the Query
     *                          operation
     *
     * @return An asynchronous iterator that yields query results. Intended
     * to be consumed with a `for await ... of` loop.
     */
    query<T extends StringToAnyObjectMap = StringToAnyObjectMap>(
        valueConstructor: ZeroArgumentsConstructor<T>,
        keyCondition: ConditionExpression |
            {[propertyName: string]: ConditionExpressionPredicate|any},
        options?: QueryOptions
    ): AsyncIterableIterator<T>;

    /**
     * @deprecated
     */
    query<T extends StringToAnyObjectMap = StringToAnyObjectMap>(
        parameters: QueryParameters<T>
    ): AsyncIterableIterator<T>;

    async *query<T extends StringToAnyObjectMap = StringToAnyObjectMap>(
        valueConstructorOrParameters: ZeroArgumentsConstructor<T>|QueryParameters<T>,
        keyCondition?: ConditionExpression |
            {[propertyName: string]: ConditionExpressionPredicate|any},
        options: QueryOptions = {}
    ) {
        let valueConstructor: ZeroArgumentsConstructor<T>;
        if (!keyCondition) {
            valueConstructor = (valueConstructorOrParameters as QueryParameters<T>).valueConstructor;
            keyCondition = (valueConstructorOrParameters as QueryParameters<T>).keyCondition;
            options = (valueConstructorOrParameters as QueryParameters<T>);
        } else {
            valueConstructor = valueConstructorOrParameters as ZeroArgumentsConstructor<T>;
        }
        let {
            filter,
            indexName,
            limit,
            pageSize = limit,
            projection,
            readConsistency = this.readConsistency,
            scanIndexForward,
            startKey,
        } = options;

        const req: QueryInput = {
            TableName: this.getTableName(valueConstructor.prototype),
            ConsistentRead: readConsistency === 'strong',
            ScanIndexForward: scanIndexForward,
            Limit: pageSize,
            IndexName: indexName,
        };

        const schema = getSchema(valueConstructor.prototype);

        const attributes = new ExpressionAttributes();
        req.KeyConditionExpression = serializeConditionExpression(
            normalizeConditionExpressionPaths(
                normalizeKeyCondition(keyCondition),
                schema
            ),
            attributes
        );

        if (filter) {
            req.FilterExpression = serializeConditionExpression(
                normalizeConditionExpressionPaths(filter, schema),
                attributes
            );
        }

        if (projection) {
            req.ProjectionExpression = serializeProjectionExpression(
                projection.map(propName => toSchemaName(propName, schema)),
                attributes
            );
        }

        if (Object.keys(attributes.names).length > 0) {
            req.ExpressionAttributeNames = attributes.names;
        }

        if (Object.keys(attributes.values).length > 0) {
            req.ExpressionAttributeValues = attributes.values;
        }

        if (startKey) {
            req.ExclusiveStartKey = marshallKey(schema, startKey, indexName);
        }

        let result: QueryOutput;
        do {
            result = await this.client.query(req).promise();
            req.ExclusiveStartKey = result.LastEvaluatedKey;
            if (result.Items) {
                for (const item of result.Items) {
                    yield unmarshallItem<T>(schema, item, valueConstructor);
                }
            }
        } while (result.LastEvaluatedKey !== undefined);
    }

    /**
     * Perform a Scan operation using the schema accessible via the
     * {DynamoDbSchema} method and the table name accessible via the
     * {DynamoDbTable} method on the prototype of the constructor supplied.
     *
     * @param valueConstructor  The constructor to use for each item returned by
     *                          the Scan operation.
     * @param options           Additional options for customizing the Scan
     *                          operation
     *
     * @return An asynchronous iterator that yields scan results. Intended
     * to be consumed with a `for await ... of` loop.
     */
    scan<T extends StringToAnyObjectMap>(
        valueConstructor: ZeroArgumentsConstructor<T>,
        options?: ScanOptions|ParallelScanWorkerOptions
    ): AsyncIterableIterator<T>;

    /**
     * @deprecated
     */
    scan<T extends StringToAnyObjectMap>(
        parameters: ScanParameters<T>|ParallelScanWorkerParameters<T>
    ): AsyncIterableIterator<T>;

    async *scan<T extends StringToAnyObjectMap>(
        ctorOrParams: ZeroArgumentsConstructor<T> |
                      ScanParameters<T> |
                      ParallelScanWorkerParameters<T>,
        options: ScanOptions|ParallelScanWorkerOptions = {}
    ): AsyncIterableIterator<T> {
        let valueConstructor: ZeroArgumentsConstructor<T>;
        if (
            'valueConstructor' in ctorOrParams &&
            (ctorOrParams as ScanParameters<T>).valueConstructor.prototype &&
            (ctorOrParams as ScanParameters<T>).valueConstructor.prototype[DynamoDbTable]
        ) {
            valueConstructor = (ctorOrParams as ScanParameters<T>).valueConstructor;
            options = ctorOrParams as ScanParameters<T>;
        } else {
            valueConstructor = ctorOrParams as ZeroArgumentsConstructor<T>;
        }

        const req = this.buildScanInput(valueConstructor, options);
        const schema = getSchema(valueConstructor.prototype);

        yield* this.doSequentialScan(
            req,
            schema,
            valueConstructor as ZeroArgumentsConstructor<T>
        );
    }

    /**
     * Perform an UpdateItem operation using the schema accessible via the
     * {DynamoDbSchema} method and the table name accessible via the
     * {DynamoDbTable} method on the item supplied.
     *
     * @param item      The item to save to DynamoDB
     * @param options   Options to configure the UpdateItem operation
     */
    update<T extends StringToAnyObjectMap = StringToAnyObjectMap>(
        item: T,
        options?: UpdateOptions
    ): Promise<T>;

    /**
     * @deprecated
     */
    update<T extends StringToAnyObjectMap = StringToAnyObjectMap>(
        parameters: UpdateParameters<T>
    ): Promise<T>;

    async update<T extends StringToAnyObjectMap = StringToAnyObjectMap>(
        itemOrParameters: T|UpdateParameters<T>,
        options: UpdateOptions = {}
    ): Promise<T> {
        let item: T;
        if (
            'item' in itemOrParameters &&
            (itemOrParameters as UpdateParameters<T>).item[DynamoDbTable]
        ) {
            item = (itemOrParameters as UpdateParameters<T>).item;
            options = itemOrParameters as UpdateParameters<T>;
        } else {
            item = itemOrParameters as T;
        }
        let {
            condition,
            onMissing = 'remove',
            skipVersionCheck = this.skipVersionCheck,
        } = options;

        const schema = getSchema(item);
        const req: UpdateItemInput = {
            TableName: this.getTableName(item),
            ReturnValues: 'ALL_NEW',
            Key: marshallKey(schema, item),
        };

        const attributes = new ExpressionAttributes();
        const expr = new UpdateExpression();

        for (const key of Object.keys(schema)) {
            let inputMember = item[key];
            const fieldSchema = schema[key];
            const {attributeName = key} = fieldSchema;

            if (isKey(fieldSchema)) {
                // Keys must be excluded from the update expression
                continue;
            } else if (isVersionAttribute(fieldSchema)) {
                const {condition: versionCond, value} = handleVersionAttribute(
                    attributeName,
                    inputMember
                );
                expr.set(attributeName, value);

                if (!skipVersionCheck) {
                    condition = condition
                        ? {type: 'And', conditions: [condition, versionCond]}
                        : versionCond;
                }
            } else if (inputMember === undefined) {
                if (onMissing === 'remove') {
                    expr.remove(attributeName);
                }
            } else {
                const marshalled = marshallValue(fieldSchema, inputMember);
                if (marshalled) {
                    expr.set(attributeName, new AttributeValue(marshalled));
                }
            }
        }

        if (condition) {
            req.ConditionExpression = serializeConditionExpression(
                normalizeConditionExpressionPaths(
                    condition,
                    schema
                ),
                attributes
            );
        }

        req.UpdateExpression = expr.serialize(attributes);
        if (Object.keys(attributes.names).length > 0) {
            req.ExpressionAttributeNames = attributes.names;
        }

        if (Object.keys(attributes.values).length > 0) {
            req.ExpressionAttributeValues = attributes.values;
        }

        const rawResponse = await this.client.updateItem(req).promise();
        if (rawResponse.Attributes) {
            return unmarshallItem<T>(
                schema,
                rawResponse.Attributes,
                item.constructor as ZeroArgumentsConstructor<T>
            );
        }

        // this branch should not be reached when interacting with DynamoDB, as
        // the ReturnValues parameter is hardcoded to 'ALL_NEW' above. It is,
        // however, allowed by the service model and may therefore occur in
        // certain unforeseen conditions; to be safe, this case should be
        // converted into an error unless a compelling reason to return
        // undefined or an empty object presents itself.
        throw new Error(
            'Update operation completed successfully, but the updated value was not returned'
        );
    }

    private buildScanInput(
        valueConstructor: ZeroArgumentsConstructor<any>,
        {
            filter,
            indexName,
            limit,
            pageSize = limit,
            projection,
            readConsistency = this.readConsistency,
            segment,
            startKey,
            totalSegments,
        }: ScanOptions|ParallelScanWorkerOptions
    ): ScanInput {
        const req: ScanInput = {
            TableName: this.getTableName(valueConstructor.prototype),
            ConsistentRead: readConsistency === 'strong',
            Limit: pageSize,
            IndexName: indexName,
            Segment: segment,
            TotalSegments: totalSegments,
        };

        const schema = getSchema(valueConstructor.prototype);

        const attributes = new ExpressionAttributes();

        if (filter) {
            req.FilterExpression = serializeConditionExpression(
                normalizeConditionExpressionPaths(filter, schema),
                attributes
            );
        }

        if (projection) {
            req.ProjectionExpression = serializeProjectionExpression(
                projection.map(propName => toSchemaName(propName, schema)),
                attributes
            );
        }

        if (Object.keys(attributes.names).length > 0) {
            req.ExpressionAttributeNames = attributes.names;
        }

        if (Object.keys(attributes.values).length > 0) {
            req.ExpressionAttributeValues = attributes.values;
        }

        if (startKey) {
            req.ExclusiveStartKey = marshallKey(schema, startKey, indexName);
        }

        return req;
    }

    private async *doSequentialScan<T>(
        req: ScanInput,
        schema: Schema,
        ctor: ZeroArgumentsConstructor<T>
    ) {
        let result: ScanOutput;
        do {
            result = await this.client.scan(req).promise();
            req.ExclusiveStartKey = result.LastEvaluatedKey;
            if (result.Items) {
                for (const item of result.Items) {
                    yield unmarshallItem(schema, item, ctor);
                }
            }
        } while (result.LastEvaluatedKey !== undefined);
    }

    private getTableName(item: StringToAnyObjectMap): string {
        return getTableName(item, this.tableNamePrefix);
    }

    private async *mapGetBatch<T extends StringToAnyObjectMap>(
        items: SyncOrAsyncIterable<T>,
        state: BatchState<T>,
        options: {[tableName: string]: BatchGetTableOptions},
        convertedOptions: PerTableOptions
    ): AsyncIterableIterator<[string, AttributeMap]> {
        for await (const item of items) {
            const unprefixed = getTableName(item);
            const tableName = this.tableNamePrefix + unprefixed;
            const schema = getSchema(item);

            if (unprefixed in options && !(tableName in convertedOptions)) {
                convertedOptions[tableName] = convertBatchGetOptions(
                    options[unprefixed],
                    schema
                );
            }

            if (!(tableName in state)) {
                state[tableName] = {
                    keyProperties: getKeyProperties(schema),
                    itemSchemata: {}
                };
            }

            const {keyProperties, itemSchemata} = state[tableName];
            const marshalled = marshallKey(schema, item);
            itemSchemata[itemIdentifier(marshalled, keyProperties)] = {
                constructor: item.constructor as ZeroArgumentsConstructor<T>,
                schema,
            };

            yield [tableName, marshalled];
        }
    }

    private async *mapWriteBatch<T extends StringToAnyObjectMap>(
        items: SyncOrAsyncIterable<[WriteType, T]>,
        state: BatchState<T>
    ): AsyncIterableIterator<[string, WriteRequest]> {
        for await (const [type, item] of items) {
            const unprefixed = getTableName(item);
            const tableName = this.tableNamePrefix + unprefixed;
            const schema = getSchema(item);

            if (!(tableName in state)) {
                state[tableName] = {
                    keyProperties: getKeyProperties(schema),
                    itemSchemata: {}
                };
            }

            const {keyProperties, itemSchemata} = state[tableName];
            const attributes = type === 'delete'
                ? marshallKey(schema, item)
                : marshallItem(schema, item);
            const marshalled = type === 'delete'
                ? {DeleteRequest: {Key: attributes}}
                : {PutRequest: {Item: attributes}}
            itemSchemata[itemIdentifier(attributes, keyProperties)] = {
                constructor: item.constructor as ZeroArgumentsConstructor<T>,
                schema,
            };

            yield [tableName, marshalled];
        }
    }
}

function attributeDefinitionList(
    attributes: AttributeTypeMap
): Array<AttributeDefinition> {
    return Object.keys(attributes).map(name => ({
        AttributeName: name,
        AttributeType: attributes[name]
    }));
}

function convertBatchGetOptions(
    options: BatchGetTableOptions,
    itemSchema: Schema
): TableOptions {
    const out: TableOptions = {};

    if (options.readConsistency) {
        out.ConsistentRead = options.readConsistency === 'strong';
    }

    if (options.projection) {
        const attributes = new ExpressionAttributes();
        out.ProjectionExpression = serializeProjectionExpression(
            options.projection.map(
                propName => toSchemaName(
                    propName,
                    options.projectionSchema || itemSchema
                )
            ),
            attributes
        );
        out.ExpressionAttributeNames = attributes.names;
    }

    return out;
}

function getKeyProperties(schema: Schema): Array<string> {
    const keys: Array<string> = [];
    for (const property of Object.keys(schema).sort()) {
        const fieldSchema = schema[property];
        if (isKey(fieldSchema)) {
            keys.push(fieldSchema.attributeName || property);
        }
    }

    return keys;
}

function handleVersionAttribute(
    attributeName: string,
    inputMember: any,
): {condition: ConditionExpression, value: MathematicalExpression|AttributeValue} {
    let condition: ConditionExpression;
    let value: any;
    if (inputMember === undefined) {
        condition = new FunctionExpression(
            'attribute_not_exists',
            new AttributePath([
                {type: 'AttributeName', name: attributeName} as PathElement
            ])
        );
        value = new AttributeValue({N: "0"});
    } else {
        condition = {
            type: 'Equals',
            subject: attributeName,
            object: inputMember,
        };
        value = new MathematicalExpression(
            new AttributePath(attributeName),
            '+',
            1
        );
    }

    return {condition, value};
}

function indexDefinitions(
    keys: PerIndexKeys,
    options: PerIndexOptions,
    schema: Schema
): {
    GlobalSecondaryIndexes?: GlobalSecondaryIndexList;
    LocalSecondaryIndexes?: LocalSecondaryIndexList;
} {
    const globalIndices: GlobalSecondaryIndexList = [];
    const localIndices: LocalSecondaryIndexList = [];

    for (const IndexName of Object.keys(keys)) {
        const KeySchema = keyTypesToElementList(keys[IndexName]);
        const indexOptions = options[IndexName];
        if (!indexOptions) {
            throw new Error(`No options provided for ${IndexName} index`);
        }

        const indexInfo = {
            IndexName,
            KeySchema,
            Projection: indexProjection(schema, indexOptions.projection),
        };
        if (indexOptions.type === 'local') {
            localIndices.push(indexInfo);
        } else {
            globalIndices.push({
                ...indexInfo,
                ProvisionedThroughput: {
                    ReadCapacityUnits: indexOptions.readCapacityUnits,
                    WriteCapacityUnits: indexOptions.writeCapacityUnits,
                },
            });
        }
    }

    return {
        GlobalSecondaryIndexes: globalIndices.length ? globalIndices : void 0,
        LocalSecondaryIndexes: localIndices.length ? localIndices : void 0,
    };
}

function indexProjection(
    schema: Schema,
    projection: SecondaryIndexProjection
): Projection {
    if (typeof projection === 'string') {
        return {
            ProjectionType:  projection === 'all' ? 'ALL' : 'KEYS_ONLY',
        }
    }

    return {
        ProjectionType: 'INCLUDE',
        NonKeyAttributes: projection.map(propName => getSchemaName(propName, schema))
    };
}

function isIterable<T>(arg: any): arg is Iterable<T> {
    return Boolean(arg) && typeof arg[Symbol.iterator] === 'function';
}

function isVersionAttribute(fieldSchema: SchemaType): boolean {
    return fieldSchema.type === 'Number'
        && Boolean(fieldSchema.versionAttribute);
}

function itemIdentifier(
    marshalled: AttributeMap,
    keyProperties: Array<string>
): string {
    const keyAttributes: Array<string> = [];
    for (const key of keyProperties) {
        const value = marshalled[key];
        `${key}=${value.B || value.N || value.S}`;
    }

    return keyAttributes.join(':');
}

function keyTypesToElementList(keys: KeyTypeMap): Array<KeySchemaElement> {
    return Object.keys(keys).map(name => ({
        AttributeName: name,
        KeyType: keys[name]
    }));
}

function normalizeConditionExpressionPaths(
    expr: ConditionExpression,
    schema: Schema
): ConditionExpression {
    if (FunctionExpression.isFunctionExpression(expr)) {
        return new FunctionExpression(
            expr.name,
            ...expr.args.map(arg => normalizeIfPath(arg, schema))
        );
    }

    switch (expr.type) {
        case 'Equals':
        case 'NotEquals':
        case 'LessThan':
        case 'LessThanOrEqualTo':
        case 'GreaterThan':
        case 'GreaterThanOrEqualTo':
            return {
                ...expr,
                subject: toSchemaName(expr.subject, schema),
                object: normalizeIfPath(expr.object, schema),
            };

        case 'Between':
            return {
                ...expr,
                subject: toSchemaName(expr.subject, schema),
                lowerBound: normalizeIfPath(expr.lowerBound, schema),
                upperBound: normalizeIfPath(expr.upperBound, schema),
            };
        case 'Membership':
            return {
                ...expr,
                subject: toSchemaName(expr.subject, schema),
                values: expr.values.map(arg => normalizeIfPath(arg, schema)),
            };
        case 'Not':
            return {
                ...expr,
                condition: normalizeConditionExpressionPaths(
                    expr.condition,
                    schema
                ),
            };
        case 'And':
        case 'Or':
            return {
                ...expr,
                conditions: expr.conditions.map(condition =>
                    normalizeConditionExpressionPaths(condition, schema)
                ),
            };
    }
}

function normalizeIfPath(path: any, schema: Schema): any {
    if (AttributePath.isAttributePath(path)) {
        return toSchemaName(path, schema);
    }

    return path;
}

function normalizeKeyCondition(
    keyCondition: ConditionExpression |
        {[key: string]: ConditionExpressionPredicate|any}
): ConditionExpression {
    if (isConditionExpression(keyCondition)) {
        return keyCondition;
    }

    const conditions: Array<ConditionExpression> = [];
    for (const property of Object.keys(keyCondition)) {
        const predicate = keyCondition[property];
        if (isConditionExpressionPredicate(predicate)) {
            conditions.push({
                ...predicate,
                subject: property,
            });
        } else {
            conditions.push({
                type: 'Equals',
                subject: property,
                object: predicate,
            });
        }
    }

    if (conditions.length === 1) {
        return conditions[0];
    }

    return {type: 'And', conditions};
}
