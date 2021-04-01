import {ItemNotFoundException} from "./ItemNotFoundException";
import {GetItemInput} from "@aws-sdk/client-dynamodb";

describe('ItemNotFoundException', () => {
    it('should include the request sent as part of the error', () => {
        const getItemInput: GetItemInput = {
            TableName: 'foo',
            Key: {
                fizz: {S: 'buzz'},
            },
        };

        const exception = new ItemNotFoundException(getItemInput, 'message');
        expect(exception.message).toBe('message');
        expect(exception.itemSought).toBe(getItemInput);
    });

    it('should identify itself by name', () => {
        const exception = new ItemNotFoundException({} as any, 'message');
        expect(exception.name).toBe('ItemNotFoundException');
    });

    it(
        'should construct a default message from the item sought if no message supplied',
        () => {
            const exception = new ItemNotFoundException({Key: {foo: {S: "bar"}}, TableName: "MyTable"});
            expect(exception.message).toBe(
                'No item with the key {"foo":{"S":"bar"}} found in the MyTable table.'
            );
        }
    );
});
