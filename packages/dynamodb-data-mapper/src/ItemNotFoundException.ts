import { GetItemInput } from "aws-sdk/clients/dynamodb";
const {
    setPrototypeOf = function (obj: any, proto: any) {
        obj.__proto__ = proto;
        return obj;
    },
} = Object;

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

        // https://github.com/Microsoft/TypeScript/wiki/Breaking-Changes#extending-built-ins-like-error-array-and-map-may-no-longer-work
        setPrototypeOf(this, ItemNotFoundException.prototype);
    }
}

function defaultErrorMessage(itemSought: GetItemInput): string {
    return `No item with the key ${
        JSON.stringify(itemSought.Key)
        } found in the ${itemSought.TableName} table.`;
}
