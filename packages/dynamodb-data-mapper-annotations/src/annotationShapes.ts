import {ZeroArgumentsConstructor} from '@aws/dynamodb-data-marshaller';

export interface ClassAnnotation {
    (target: ZeroArgumentsConstructor<any>): void;
}

export interface PropertyAnnotation {
    (target: Object, propertyKey: string|symbol): void;
}