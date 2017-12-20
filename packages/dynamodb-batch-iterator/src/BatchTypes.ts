import {
    AttributeMap,
    ConsistentRead,
    ExpressionAttributeNameMap,
    ProjectionExpression,
} from "aws-sdk/clients/dynamodb";

export type SyncOrAsyncIterable<T> = Iterable<T>|AsyncIterable<T>;

/**
 * @internal
 */
export interface BatchState<Element extends TableStateElement> {
    [tableName: string]: TableState<Element>;
}

/**
 * @internal
 */
export interface TableState<Element extends TableStateElement> {
    attributeNames?: ExpressionAttributeNameMap;
    backoffFactor: number;
    consistentRead?: ConsistentRead;
    name: string;
    projection?: ProjectionExpression;
    tableThrottling?: TableThrottlingTracker<Element>;
}

/**
 * @internal
 */
export type TableStateElement = AttributeMap|WritePair;

/**
 * @internal
 */
export interface TableThrottlingTracker<Element extends TableStateElement> {
    backoffWaiter: Promise<ThrottledTableConfiguration<Element>>;
    unprocessed: Array<Element>;
}

/**
 * @internal
 */
export interface ThrottledTableConfiguration<
    Element extends TableStateElement
> extends TableState<Element> {
    tableThrottling: TableThrottlingTracker<Element>;
}

/**
 * @internal
 */
export type WriteType = 'put'|'delete';

/**
 * @internal
 */
export type WritePair = [WriteType, AttributeMap];
