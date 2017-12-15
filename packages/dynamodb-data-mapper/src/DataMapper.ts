import {
    MAX_READ_BATCH_SIZE,
    MAX_WRITE_BATCH_SIZE,
    ReadConsistency,
    SyncOrAsyncIterable,
    VERSION,
    WriteType,
} from "./constants";
import {ItemNotFoundException} from "./ItemNotFoundException";
import {
    BaseScanOptions,
    BatchGetOptions,
    DataMapperConfiguration,
    DeleteOptions,
    DeleteParameters,
    GetOptions,
    GetParameters,
    ParallelScanParameters,
    ParallelScanWorkerOptions,
    ParallelScanWorkerParameters,
    PutOptions,
    PutParameters,
    QueryOptions,
    QueryParameters,
    ScanOptions,
    ScanParameters,
    StringToAnyObjectMap,
    UpdateOptions,
    UpdateParameters,
} from './namedParameters';
import {
    DynamoDbTable,
    getSchema,
    getTableName,
} from './protocols';
import {
    isKey,
    marshallItem,
    marshallKey,
    marshallValue,
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
    serializeConditionExpression,
    serializeProjectionExpression,
    UpdateExpression,
} from "@aws/dynamodb-expressions";
import {
    AttributeMap,
    BatchGetItemInput,
    BatchWriteItemInput,
    DeleteItemInput,
    GetItemInput,
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
     * tables and schemas for each item are determined using the schema
     * {DynamoDbSchema} property and the {DynamoDbTable} property on defined on
     * each item supplied.
     *
     * This method will automatically retry any delete requests returned by
     * DynamoDB as unprocessed. Exponential backoff on unprocessed items is
     * employed on a per-table basis.
     *
     * @param items A synchronous or asynchronous iterable of items to delete.
     */
    async batchDelete<T extends StringToAnyObjectMap>(
        items: SyncOrAsyncIterable<T>
    ) {
        const iter = this.batchWrite(
            async function *mapToDelete(): AsyncIterable<['delete', T]> {
                for await (const item of items) {
                    yield ['delete', item];
                }
            }()
        );

        for await (const _ of iter) {
            // nothing is returned for deletes, but the iterator must be
            // exhausted to ensure all deletions have been submitted
        }
    }

    /**
     * Retrieves items from DynamoDB in batches of 100 or fewer via one or more
     * BatchGetItem operations. The items may be from any number of tables;
     * tables and schemas for each item are determined using the schema
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
        options: BatchGetOptions = {}
    ) {
        const pending: Array<[string, AttributeMap]> = [];
        const throttled = new Set<Promise<ThrottledTableConfiguration<T, AttributeMap>>>();
        const {
            readConsistency = this.readConsistency,
            perTableOptions = {},
        } = options;
        const tableConfigurations: {
            [key: string]: TableConfiguration<T, AttributeMap>
        } = {};

        if (isIterable(items)) {
            yield* this.batchGetSync(
                items,
                pending,
                throttled,
                tableConfigurations,
                perTableOptions,
                readConsistency
            );
        } else {
            yield* this.batchGetAsync(
                items,
                pending,
                throttled,
                tableConfigurations,
                perTableOptions,
                readConsistency
            );
        }

        while (pending.length > 0 || throttled.size > 0) {
            while (pending.length < MAX_READ_BATCH_SIZE && throttled.size > 0) {
                this.enqueueThrottledItems(
                    await Promise.race(throttled),
                    pending,
                    throttled
                );
            }

            yield* this.flushPendingReads(
                pending,
                throttled,
                tableConfigurations
            );
        }
    }

    /**
     * Puts items into DynamoDB in batches of 25 or fewer via one or more
     * BatchWriteItem operations. The items may be from any number of tables;
     * tables and schemas for each item are determined using the schema
     * {DynamoDbSchema} property and the {DynamoDbTable} property on defined on
     * each item supplied.
     *
     * This method will automatically retry any put requests returned by
     * DynamoDB as unprocessed. Exponential backoff on unprocessed items is
     * employed on a per-table basis.
     *
     * @param items A synchronous or asynchronous iterable of items to put.
     */
    batchPut<T extends StringToAnyObjectMap>(
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

        return this.batchWrite(generator);
    }

    /**
     * Puts or deletes items from DynamoDB in batches of 25 or fewer via one or
     * more BatchWriteItem operations. The items may belong to any number of
     * tables; tables and schemas for each item are determined using the schema
     * {DynamoDbSchema} property and the {DynamoDbTable} property on defined on
     * each item supplied.
     *
     * This method will automatically retry any write requests returned by
     * DynamoDB as unprocessed. Exponential backoff on unprocessed items is
     * employed on a per-table basis.
     *
     * @param items A synchronous or asynchronous iterable of tuples of the
     * string 'Put'|'Delete' and the item on which to perform the specified
     * write action.
     */
    async *batchWrite<T extends StringToAnyObjectMap>(
        items: SyncOrAsyncIterable<[WriteType, T]>
    ) {
        const pending: Array<[string, WritePair]> = [];
        const throttled = new Set<Promise<ThrottledTableConfiguration<T, WritePair>>>();
        const configs: {[key: string]: TableConfiguration<T, WritePair>} = {};

        if (isIterable(items)) {
            yield* this.batchWriteSync(items, pending, throttled, configs);
        } else {
            yield* this.batchWriteAsync(items, pending, throttled, configs);
        }

        while (pending.length > 0 || throttled.size > 0) {
            while (pending.length < MAX_WRITE_BATCH_SIZE && throttled.size > 0) {
                this.enqueueThrottledItems(
                    await Promise.race(throttled),
                    pending,
                    throttled
                );
            }

            yield* this.flushPendingWrites(pending, throttled, configs);
        }
    }

    /**
     * Perform a DeleteItem operation using the schema accessible via the
     * {DynamoDbSchema} property and the table name accessible via the
     * {DynamoDbTable} property on the item supplied.
     *
     * @param item The item to delete
     * @param options Options to configure the DeleteItem operation
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

        const operationInput: DeleteItemInput = {
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
            operationInput.ConditionExpression = serializeConditionExpression(
                normalizeConditionExpressionPaths(condition, schema),
                attributes
            );
            operationInput.ExpressionAttributeNames = attributes.names;
            operationInput.ExpressionAttributeValues = attributes.values;
        }

        const {Attributes} = await this.client.deleteItem(operationInput).promise();
        if (Attributes) {
            return unmarshallItem<T>(
                schema,
                Attributes,
                item.constructor as ZeroArgumentsConstructor<T>
            );
        }
    }

    /**
     * Perform a GetItem operation using the schema accessible via the
     * {DynamoDbSchema} method and the table name accessible via the
     * {DynamoDbTable} method on the item supplied.
     *
     * @param item The item to get
     * @param options Options to configure the GetItem operation
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
        const operationInput: GetItemInput = {
            TableName: this.getTableName(item),
            Key: marshallKey(schema, item),
            ConsistentRead: readConsistency === 'strong',
        };

        if (projection) {
            const attributes = new ExpressionAttributes();
            operationInput.ProjectionExpression = serializeProjectionExpression(
                projection.map(propName => toSchemaName(propName, schema)),
                attributes
            );
            operationInput.ExpressionAttributeNames = attributes.names;
        }

        const {Item} = await this.client.getItem(operationInput).promise();
        if (Item) {
            return unmarshallItem<T>(
                schema,
                Item,
                item.constructor as ZeroArgumentsConstructor<T>
            );
        }

        throw new ItemNotFoundException(operationInput);
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
     * @param item The item to save to DynamoDB
     * @param options Options to configure the PutItem operation
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
            req.ExpressionAttributeNames = attributes.names;
            req.ExpressionAttributeValues = attributes.values;
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

        req.ExpressionAttributeNames = attributes.names;
        req.ExpressionAttributeValues = attributes.values;

        if (startKey) {
            req.ExclusiveStartKey = marshallItem(schema, startKey);
        }

        let result: QueryOutput;
        do {
            result = await this.client.query(req).promise();
            req.ExclusiveStartKey = result.LastEvaluatedKey;
            if (result.Items) {
                for (const item of result.Items) {
                    yield unmarshallItem<T>(schema, item);
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
     * @param item The item to save to DynamoDB
     * @param options Options to configure the UpdateItem operation
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
        req.ExpressionAttributeNames = attributes.names;
        req.ExpressionAttributeValues = attributes.values;

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

    private addPendingRead<T>(
        item: T,
        pending: Array<[string, AttributeMap]>,
        tableConfigs: {[key: string]: TableConfiguration<T, AttributeMap>},
        perTableOptions: {[key: string]: GetOptions},
        defaultReadConsistency: ReadConsistency
    ): void {
        const schema = getSchema(item);
        const tableName = this.getTableName(item);
        if (!(tableName in tableConfigs)) {
            const {
                projection,
                readConsistency = defaultReadConsistency,
            } = perTableOptions[tableName] || {} as GetOptions;

            tableConfigs[tableName] = {
                backoffFactor: 0,
                keyProperties: getKeyProperties(schema),
                name: tableName,
                readConsistency,
                itemConfigurations: {}
            };

            if (projection) {
                const attributes = new ExpressionAttributes();
                tableConfigs[tableName].projection = serializeProjectionExpression(
                    projection.map(propName => toSchemaName(propName, schema)),
                    attributes
                );
                tableConfigs[tableName].attributeNames = attributes.names;
            }
        }

        const tableData = tableConfigs[tableName];
        const marshalled = marshallItem(schema, item);
        const identifier = itemIdentifier(marshalled, tableData.keyProperties);
        tableData.itemConfigurations[identifier] = {
            schema,
            constructor: item.constructor as ZeroArgumentsConstructor<T>,
        };

        if (tableData.tableThrottling) {
            tableData.tableThrottling.unprocessed.push(marshalled);
        } else {
            pending.push([tableName, marshalled]);
        }
    }

    private addPendingWrite<T>(
        type: WriteType,
        item: T,
        pending: Array<[string, WritePair]>,
        tableConfigs: {[key: string]: TableConfiguration<T, WritePair>}
    ): void {
        const schema = getSchema(item);
        const tableName = this.getTableName(item);
        if (!(tableName in tableConfigs)) {
            tableConfigs[tableName] = {
                backoffFactor: 0,
                keyProperties: getKeyProperties(schema),
                name: tableName,
                itemConfigurations: {}
            };
        }

        const tableData = tableConfigs[tableName];
        const marshalled = type === 'delete'
            ? marshallKey(schema, item)
            : marshallItem(schema, item);
        const identifier = itemIdentifier(marshalled, tableData.keyProperties);
        tableData.itemConfigurations[identifier] = {
            schema,
            constructor: item.constructor as ZeroArgumentsConstructor<T>,

        };

        if (tableData.tableThrottling) {
            tableData.tableThrottling.unprocessed.push([type, marshalled]);
        } else {
            pending.push([tableName, [type, marshalled]]);
        }
    }

    private async *batchGetAsync<T>(
        items: AsyncIterable<T>,
        pending: Array<[string, AttributeMap]>,
        throttled: Set<Promise<ThrottledTableConfiguration<T, AttributeMap>>>,
        tableConfigs: {[tableName: string]: TableConfiguration<T, AttributeMap>},
        perTableOptions: {[tableName: string]: GetOptions},
        readConsistency: ReadConsistency
    ) {
        const iterator = items[Symbol.asyncIterator]();
        let next = iterator.next();
        let done = false;

        while (!done) {
            const toProcess = await Promise.race([
                next,
                Promise.race(throttled)
            ]);

            if (isIteratorResult(toProcess)) {
                done = toProcess.done;
                if (!done) {
                    this.addPendingRead(
                        toProcess.value,
                        pending,
                        tableConfigs,
                        perTableOptions,
                        readConsistency
                    );
                    next = iterator.next();
                }
            } else {
                this.enqueueThrottledItems(toProcess, pending, throttled);
            }

            if (pending.length >= MAX_READ_BATCH_SIZE) {
                yield* this.flushPendingReads(pending, throttled, tableConfigs);
            }
        }
    }

    private async *batchGetSync<T>(
        items: Iterable<T>,
        pending: Array<[string, AttributeMap]>,
        throttled: Set<Promise<ThrottledTableConfiguration<T, AttributeMap>>>,
        configs: {[tableName: string]: TableConfiguration<T, AttributeMap>},
        options: {[tableName: string]: GetOptions},
        consistency: ReadConsistency
    ) {
        for (const item of items) {
            this.addPendingRead(item, pending, configs, options, consistency);

            if (pending.length >= MAX_READ_BATCH_SIZE) {
                yield* this.flushPendingReads(pending, throttled, configs);
            }
        }
    }

    private async *batchWriteAsync<T>(
        items: AsyncIterable<[WriteType, T]>,
        pending: Array<[string, WritePair]>,
        throttled: Set<Promise<ThrottledTableConfiguration<T, WritePair>>>,
        configs: {[tableName: string]: TableConfiguration<T, WritePair>},
    ) {
        const iterator = items[Symbol.asyncIterator]();
        let next = iterator.next();
        let done = false;

        while (!done) {
            const toProcess = await Promise.race([
                next,
                Promise.race(throttled)
            ]);

            if (isIteratorResult(toProcess)) {
                done = toProcess.done;
                if (!done) {
                    const [type, item] = toProcess.value;
                    this.addPendingWrite(type, item, pending, configs);
                    next = iterator.next();
                }
            } else {
                this.enqueueThrottledItems(toProcess, pending, throttled);
            }

            if (pending.length >= MAX_WRITE_BATCH_SIZE) {
                yield* this.flushPendingWrites(pending, throttled, configs);
            }
        }
    }

    private async *batchWriteSync<T>(
        items: Iterable<[WriteType, T]>,
        pending: Array<[string, WritePair]>,
        throttled: Set<Promise<ThrottledTableConfiguration<T, WritePair>>>,
        configs: {[tableName: string]: TableConfiguration<T, WritePair>},
    ) {
        for (const [type, item] of items) {
            this.addPendingWrite(type, item, pending, configs);

            if (pending.length === MAX_WRITE_BATCH_SIZE) {
                yield* this.flushPendingWrites(pending, throttled, configs);
            }
        }
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
            req.ExclusiveStartKey = marshallItem(schema, startKey);
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

    private enqueueThrottledItems<T, E extends TableConfigurationElement>(
        table: ThrottledTableConfiguration<T, E>,
        pending: Array<[string, E]>,
        throttled: Set<Promise<ThrottledTableConfiguration<T, E>>>
    ): void {
        const {
            tableThrottling: {backoffWaiter, unprocessed}
        } = table;
        if (unprocessed.length > 0) {
            pending.push(...unprocessed.map(
                attr => [table.name, attr] as [string, E]
            ));
        }

        throttled.delete(backoffWaiter);
        delete table.tableThrottling;
    }

    private async *flushPendingReads<T>(
        toFlush: Array<[string, AttributeMap]>,
        throttled: Set<Promise<ThrottledTableConfiguration<T, AttributeMap>>>,
        tables: {[key: string]: TableConfiguration<T, AttributeMap>}
    ) {
        if (toFlush.length === 0) {
            return;
        }

        const operationInput: BatchGetItemInput = {RequestItems: {}};
        let batchSize = 0;

        while (toFlush.length > 0) {
            const [tableName, item] = toFlush.shift() as [string, AttributeMap];
            if (operationInput.RequestItems[tableName] === undefined) {
                const {
                    projection,
                    readConsistency,
                    attributeNames,
                } = tables[tableName];

                operationInput.RequestItems[tableName] = {
                    Keys: [],
                    ConsistentRead: readConsistency === 'strong',
                    ProjectionExpression: projection,
                    ExpressionAttributeNames: attributeNames,
                };
            }
            operationInput.RequestItems[tableName].Keys.push(item);

            if (++batchSize === MAX_READ_BATCH_SIZE) {
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
            const tableData = tables[table];
            tableData.backoffFactor++;
            const unprocessed = UnprocessedKeys[table].Keys;
            if (tableData.tableThrottling) {
                throttled.delete(tableData.tableThrottling.backoffWaiter);
                unprocessed.unshift(...tableData.tableThrottling.unprocessed);
            }

            tableData.tableThrottling = {
                unprocessed,
                backoffWaiter: new Promise(resolve => {
                    setTimeout(
                        resolve,
                        exponentialBackoff(tableData.backoffFactor),
                        tableData
                    );
                })
            };

            throttled.add(tableData.tableThrottling.backoffWaiter);
        }

        for (let i = toFlush.length - 1; i > -1; i--) {
            const [table, marshalled] = toFlush[i];
            if (unprocessedTables.has(table)) {
                (tables[table] as ThrottledTableConfiguration<T, AttributeMap>)
                    .tableThrottling.unprocessed.push(marshalled);
                toFlush.splice(i, 1);
            }
        }

        for (const table of Object.keys(Responses)) {
            const tableData = tables[table];
            tableData.backoffFactor = Math.max(0, tableData.backoffFactor - 1);
            for (const item of Responses[table]) {
                const identifier = itemIdentifier(item, tableData.keyProperties);
                const {
                    constructor,
                    schema,
                } = tableData.itemConfigurations[identifier];
                yield unmarshallItem<T>(schema, item, constructor);
            }
        }
    }

    private async *flushPendingWrites<T>(
        pending: Array<[string, WritePair]>,
        throttled: Set<Promise<ThrottledTableConfiguration<T, WritePair>>>,
        tables: {[key: string]: TableConfiguration<T, WritePair>}
    ) {
        const writesInFlight: Array<[string, AttributeMap]> = [];
        const operationInput: BatchWriteItemInput = {RequestItems: {}};

        let batchSize = 0;
        while (pending.length > 0) {
            const [
                tableName,
                [type, marshalled]
            ] = pending.shift() as [string, WritePair];

            if (type === 'put') {
                writesInFlight.push([tableName, marshalled]);
            }

            if (operationInput.RequestItems[tableName] === undefined) {
                operationInput.RequestItems[tableName] = [];
            }
            operationInput.RequestItems[tableName].push(
                type === 'delete'
                    ? {DeleteRequest: {Key: marshalled}}
                    : {PutRequest: {Item: marshalled}}
            );

            if (++batchSize === MAX_WRITE_BATCH_SIZE) {
                break;
            }
        }

        const {UnprocessedItems = {}} = await this.client
            .batchWriteItem(operationInput).promise();
        const unprocessedTables = new Set<string>();

        for (const table of Object.keys(UnprocessedItems)) {
            unprocessedTables.add(table);
            const tableData = tables[table];
            tableData.backoffFactor++;

            const unprocessed: Array<WritePair> = UnprocessedItems[table]
                .map(write => {
                    if (write.DeleteRequest) {
                        return ['delete', write.DeleteRequest.Key] as WritePair;
                    } else if (write.PutRequest) {
                        return ['put', write.PutRequest.Item] as WritePair;
                    }
                }).filter(
                    (el => Boolean(el)) as (arg?: WritePair) => arg is WritePair
                );

            if (tableData.tableThrottling) {
                throttled.delete(tableData.tableThrottling.backoffWaiter);
                unprocessed.unshift(...tableData.tableThrottling.unprocessed);
            }

            tableData.tableThrottling = {
                unprocessed,
                backoffWaiter: new Promise(resolve => {
                    setTimeout(
                        resolve,
                        exponentialBackoff(tableData.backoffFactor),
                        tableData
                    );
                })
            };

            throttled.add(tableData.tableThrottling.backoffWaiter);
        }

        for (const [tableName, marshalled] of writesInFlight) {
            const {keyProperties, itemConfigurations} = tables[tableName];
            const {
                constructor,
                schema,
            } = itemConfigurations[itemIdentifier(marshalled, keyProperties)];

            yield unmarshallItem<T>(schema, marshalled, constructor);
        }
    }

    private getTableName(item: StringToAnyObjectMap): string {
        return getTableName(item, this.tableNamePrefix);
    }
}

interface TableConfiguration<T, E extends TableConfigurationElement> {
    attributeNames?: {[key: string]: string};
    backoffFactor: number;
    keyProperties: Array<string>;
    name: string;
    projection?: string;
    readConsistency?: ReadConsistency;
    tableThrottling?: TableThrottlingTracker<T, E>;
    itemConfigurations: {
        [itemIdentifier: string]: {
            schema: Schema;
            constructor: ZeroArgumentsConstructor<T>;
        }
    }
}

type TableConfigurationElement = AttributeMap|WritePair;

interface TableThrottlingTracker<T, E extends TableConfigurationElement> {
    backoffWaiter: Promise<ThrottledTableConfiguration<T, E>>;
    unprocessed: Array<E>;
}

interface ThrottledTableConfiguration<
    T,
    E extends TableConfigurationElement
> extends TableConfiguration<T, E> {
    tableThrottling: TableThrottlingTracker<T, E>;
}

type WritePair = [WriteType, AttributeMap];

function exponentialBackoff(attempts: number) {
    return Math.floor(Math.random() * Math.pow(2, attempts));
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
            new AttributePath([{type: 'AttributeName', name: attributeName}])
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

function isIterable<T>(arg: any): arg is Iterable<T> {
    return Boolean(arg) && typeof arg[Symbol.iterator] === 'function';
}

function isIteratorResult<T>(arg: any): arg is IteratorResult<T> {
    return Boolean(arg) && typeof arg.done === 'boolean';
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
