import {ReadConsistency} from './constants';
import {
    Schema,
    ZeroArgumentsConstructor
} from '@aws/dynamodb-data-marshaller';
import {AttributeMap} from "aws-sdk/clients/dynamodb";

export interface BatchState<T, E extends TableStateElement> {
    [tableName: string]: TableState<T, E>;
}

export interface PreparedElement<T, E extends TableStateElement> {
    marshalled: E;
    tableName: string;
    tableState: TableState<T, E>;
}

export interface TableState<T, E extends TableStateElement> {
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

export type TableStateElement = AttributeMap|WritePair;

export interface TableThrottlingTracker<T, E extends TableStateElement> {
    backoffWaiter: Promise<ThrottledTableConfiguration<T, E>>;
    unprocessed: Array<E>;
}

export interface ThrottledTableConfiguration<
    T,
    E extends TableStateElement
> extends TableState<T, E> {
    tableThrottling: TableThrottlingTracker<T, E>;
}

export type WriteType = 'put'|'delete';

export type WritePair = [WriteType, AttributeMap];
