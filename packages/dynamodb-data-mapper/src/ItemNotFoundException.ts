import {GetItemInput} from "@aws-sdk/client-dynamodb";

/**
 * An exception thrown when an item was sought with a DynamoDB::GetItem
 * request and not found. Includes the original request sent as
 * `itemSought`.
 */
export class ItemNotFoundException extends Error {
    readonly name = 'ItemNotFoundException';

    constructor(
        public readonly itemSought: GetItemInput,
        message: string = defaultErrorMessage(itemSought)
    ) {
        super(message);
    }
}

function defaultErrorMessage(itemSought: GetItemInput): string {
    return `No item with the key ${
        JSON.stringify(itemSought.Key)
    } found in the ${itemSought.TableName} table.`;
}
