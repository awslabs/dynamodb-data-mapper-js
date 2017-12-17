import { AttributeMap } from 'aws-sdk/clients/dynamodb';

export function itemIdentifier(
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
