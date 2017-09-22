import {GetItemInput} from "aws-sdk/clients/dynamodb";

export class ItemNotFoundException extends Error {
    constructor(
        public readonly itemSought: GetItemInput,
        message?: string
    ) {
        super(message);
    }
}
