import {
    MAX_WRITE_BATCH_SIZE,
    ReadConsistency,
    SyncOrAsyncIterable,
    VERSION,
} from "./constants";
import {ItemNotFoundException} from "./ItemNotFoundException";
import {
    BaseScanOptions,
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
    DynamoDbSchema,
    DynamoDbTable,
} from './protocols';
import {
    isKey,
    marshallItem,
    marshallKey,
    marshallValue,
    Schema,
    SchemaType,
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
    BatchWriteItemInput,
    DeleteItemInput,
    GetItemInput,
    PutItemInput,
    PutRequest,
    QueryInput,
    QueryOutput,
    ScanInput,
    ScanOutput,
    UpdateItemInput,
    WriteRequest,
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

    async batchDelete<T extends StringToAnyObjectMap>(
        items: SyncOrAsyncIterable<T>
    ) {
        const iter = this.batchWrite(
            async function *mapToDelete(): AsyncIterable<['Delete', T]> {
                for await (const item of items) {
                    yield ['Delete', item];
                }
            }()
        );

        for await (const _ of iter) {
            // nothing is returned for deletes, but the iterator must be
            // exhausted to ensure all deletions have been submitted
        }
    }

    batchPut<T extends StringToAnyObjectMap>(
        items: SyncOrAsyncIterable<T>
    ) {
        return this.batchWrite(
            async function *mapToPut(): AsyncIterable<['Put', T]> {
                for await (const item of items) {
                    yield ['Put', item];
                }
            }()
        );
    }

    async *batchWrite<T extends StringToAnyObjectMap>(
        items: SyncOrAsyncIterable<['Put'|'Delete', T]>
    ) {
        const pending = new Map<string, PendingWrite<T>>();
        const throttled = new Set<Promise<void>>();

        for await (const [type, item] of items) {
            const schema = getSchema(item);
            const tableName = this.getTableName(item);
            const marshalled = type === 'Delete'
                ? marshallKey(schema, item)
                : marshallItem(schema, item);

            pending.set(`${tableName}::${itemIdentifier(marshalled)}`, {
                type,
                tableName,
                marshalled,
                schema,
                constructor: item.constructor as ZeroArgumentsConstructor<T>,
                attempts: 0
            });

            if (pending.size === MAX_WRITE_BATCH_SIZE) {
                yield* this.flushPendingWrites(pending, throttled);
            }
        }

        while (pending.size > 0 || throttled.size > 0) {
            yield* this.flushPendingWrites(pending, throttled);
            if (throttled.size > 0) {
                await Promise.race(throttled);
            }
        }
    }

    /**
     * Perform a DeleteItem operation using the schema accessible via the
     * {DynamoDbSchema} method and the table name accessible via the
     * {DynamoDbTable} method on the item supplied.
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
                normalizeConditionExpressionPaths(
                    condition,
                    getAttributeNameMapping(schema)
                ),
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
            const mapping = getAttributeNameMapping(schema);
            operationInput.ProjectionExpression = serializeProjectionExpression(
                projection.map(propName => toSchemaName(propName, mapping)),
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
                normalizeConditionExpressionPaths(
                    condition,
                    getAttributeNameMapping(schema)
                ),
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
        const mapping = getAttributeNameMapping(schema);

        req.KeyConditionExpression = serializeConditionExpression(
            normalizeConditionExpressionPaths(
                normalizeKeyCondition(keyCondition),
                mapping
            ),
            attributes
        );

        if (filter) {
            req.FilterExpression = serializeConditionExpression(
                normalizeConditionExpressionPaths(filter, mapping),
                attributes
            );
        }

        if (projection) {
            req.ProjectionExpression = serializeProjectionExpression(
                projection.map(propName => toSchemaName(propName, mapping)),
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
                    getAttributeNameMapping(schema)
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
        const mapping = getAttributeNameMapping(schema);

        if (filter) {
            req.FilterExpression = serializeConditionExpression(
                normalizeConditionExpressionPaths(filter, mapping),
                attributes
            );
        }

        if (projection) {
            req.ProjectionExpression = serializeProjectionExpression(
                projection.map(propName => toSchemaName(propName, mapping)),
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

    private async *flushPendingWrites<T>(
        pending: Map<string, PendingWrite<T>>,
        throttled: Set<Promise<void>>
    ) {
        const inFlight = new Map<string, PendingWrite<T>>();
        const operationInput: BatchWriteItemInput = {RequestItems: {}};

        for (const [identifier, data] of pending) {
            const {type, tableName, marshalled} = data;

            inFlight.set(identifier, data);
            pending.delete(identifier);

            if (operationInput.RequestItems[tableName] === undefined) {
                operationInput.RequestItems[tableName] = [];
            }
            operationInput.RequestItems[tableName].push(
                type === 'Delete'
                    ? {DeleteRequest: {Key: marshalled}}
                    : {PutRequest: {Item: marshalled}}
            );

            if (inFlight.size === MAX_WRITE_BATCH_SIZE) {
                break;
            }
        }

        const {UnprocessedItems = {}} = await this.client
            .batchWriteItem(operationInput).promise();

        for (const table of Object.keys(UnprocessedItems)) {
            for (const item of UnprocessedItems[table]) {
                const identifier = `${table}::${requestIdentifier(item)}`;
                const data = inFlight.get(identifier) as PendingWrite<T>;

                data.attempts++;
                const promise = new Promise<void>(resolve => {
                    setTimeout((data) => {
                        pending.set(identifier, data);
                        throttled.delete(promise);
                        resolve();
                    }, exponentialBackoff(data.attempts), data);
                });
                throttled.add(promise);
                inFlight.delete(identifier);
            }
        }

        for (const [_, {schema, constructor, marshalled}] of inFlight) {
            yield unmarshallItem<T>(schema, marshalled, constructor);
        }
    }

    private getTableName(item: StringToAnyObjectMap): string {
        const tableName = item[DynamoDbTable];
        if (typeof tableName === 'string') {
            return this.tableNamePrefix + tableName;
        }

        throw new Error(
            'The provided item did not adhere to the DynamoDbTable protocol. No' +
            ' string property was found at the `DynamoDbTable` symbol'
        );
    }
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

interface PendingRead<T> {
    tableName: string;
    schema: Schema;
    marshalled: AttributeMap;
    constructor: ZeroArgumentsConstructor<T>;
    attempts: number;
}

interface PendingWrite<T> extends PendingRead<T> {
    type: 'Put'|'Delete';
}

function exponentialBackoff(attempts: number) {
    return Math.floor(Math.random() * Math.pow(2, attempts));
}

type AttributeNameMapping = {[propName: string]: string};
function getAttributeNameMapping(schema: Schema): AttributeNameMapping {
    const mapping: AttributeNameMapping = {};

    for (const propName of Object.keys(schema)) {
        const {attributeName = propName} = schema[propName];
        mapping[propName] = attributeName;
    }

    return mapping;
}

function isVersionAttribute(fieldSchema: SchemaType): boolean {
    return fieldSchema.type === 'Number'
        && Boolean(fieldSchema.versionAttribute);
}

function itemIdentifier(marshalled: AttributeMap): string {
    const keyAttributes: Array<string> = [];
    for (const key of Object.keys(marshalled).sort()) {
        const value = marshalled[key];
        if (value.B || value.N || value.S) {
            keyAttributes.push(
                `${key}=${value.B || value.N || value.S}`
            );
        }
    }

    return keyAttributes.join(':');
}

function requestIdentifier(request: WriteRequest): string {
    if (request.DeleteRequest) {
        return itemIdentifier(request.DeleteRequest.Key);
    } else {
        return itemIdentifier((request.PutRequest as PutRequest).Item);
    }
}

function normalizeConditionExpressionPaths(
    expr: ConditionExpression,
    mapping: AttributeNameMapping
): ConditionExpression {
    if (FunctionExpression.isFunctionExpression(expr)) {
        return new FunctionExpression(
            expr.name,
            ...expr.args.map(arg => normalizeIfPath(arg, mapping))
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
                subject: toSchemaName(expr.subject, mapping),
                object: normalizeIfPath(expr.object, mapping),
            };

        case 'Between':
            return {
                ...expr,
                subject: toSchemaName(expr.subject, mapping),
                lowerBound: normalizeIfPath(expr.lowerBound, mapping),
                upperBound: normalizeIfPath(expr.upperBound, mapping),
            };
        case 'Membership':
            return {
                ...expr,
                subject: toSchemaName(expr.subject, mapping),
                values: expr.values.map(arg => normalizeIfPath(arg, mapping)),
            };
        case 'Not':
            return {
                ...expr,
                condition: normalizeConditionExpressionPaths(
                    expr.condition,
                    mapping
                ),
            };
        case 'And':
        case 'Or':
            return {
                ...expr,
                conditions: expr.conditions.map(condition =>
                    normalizeConditionExpressionPaths(condition, mapping)
                ),
            };
    }
}

function normalizeIfPath(path: any, mapping: AttributeNameMapping): any {
    if (AttributePath.isAttributePath(path)) {
        return toSchemaName(path, mapping);
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

function toSchemaName(
    path: AttributePath|string,
    mapping: AttributeNameMapping
): AttributePath|string {
    if (typeof path === 'string') {
        path = new AttributePath(path);
    }

    return new AttributePath(path.elements.map(el => {
        if (el.type === 'AttributeName' && el.name in mapping) {
            return {
                ...el,
                name: mapping[el.name],
            };
        }

        return el;
    }));
}

function getSchema(item: StringToAnyObjectMap): Schema {
    const schema = item[DynamoDbSchema];
    if (schema && typeof schema === 'object') {
        return schema;
    }

    throw new Error(
        'The provided item did not adhere to the DynamoDbDocument protocol.' +
        ' No object property was found at the `DynamoDbSchema` symbol'
    );
}
