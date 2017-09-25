import {GetItemInput} from "aws-sdk/clients/dynamodb";

/**
 * An exception thrown when an item was sought with a DynamoDB::GetItem
 * request and not found. Includes the original request sent as
 * `itemSought`.
 */
export class ItemNotFoundException extends Error {
    constructor(
        public readonly itemSought: GetItemInput,
        message?: string
    ) {
        super(message);
    }
}
