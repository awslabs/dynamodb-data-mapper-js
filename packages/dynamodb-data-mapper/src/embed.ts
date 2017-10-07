import {DynamoDbSchema} from "./protocols";
import {
    DocumentType,
    ZeroArgumentsConstructor,
} from '@aws/dynamodb-data-marshaller';

export interface DocumentTypeOptions<T> {
    defaultProvider?: () => T;
    attributeName?: string;
}

export function embed<T>(
    documentConstructor: ZeroArgumentsConstructor<T>,
    {attributeName, defaultProvider}: DocumentTypeOptions<T> = {}
): DocumentType {
    return {
        type: 'Document',
        members: (documentConstructor.prototype as any)[DynamoDbSchema] || {},
        attributeName,
        defaultProvider,
        valueConstructor: documentConstructor
    };
}
