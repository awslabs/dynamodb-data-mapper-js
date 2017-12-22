import { itemIdentifier } from './itemIdentifier';

describe('itemIdentifier', () => {
    it('should serialize all top-level string attributes', () => {
        expect(
            itemIdentifier('table', {DeleteRequest: {Key: {foo: {S: 'bar'}}}})
        ).toBe('table::delete::foo=bar');

        expect(
            itemIdentifier('table', {PutRequest: {Item: {foo: {S: 'bar'}}}})
        ).toBe('table::put::foo=bar');
    });

    it('should serialize all top-level number attributes', () => {
        expect(
            itemIdentifier('table', {DeleteRequest: {Key: {foo: {N: '1'}}}})
        ).toBe('table::delete::foo=1');

        expect(
            itemIdentifier('table', {PutRequest: {Item: {foo: {N: '1'}}}})
        ).toBe('table::put::foo=1');
    });

    it('should serialize all top-level binary attributes', () => {
        expect(
            itemIdentifier('table', {DeleteRequest: {Key: {foo: {B: Uint8Array.from([0xde, 0xad])}}}})
        ).toBe('table::delete::foo=222,173');

        expect(
            itemIdentifier('table', {PutRequest: {Item: {foo: {B: Uint8Array.from([0xde, 0xad])}}}})
        ).toBe('table::put::foo=222,173');
    });

    it(
        'should serialize different representations of the same binary data in the same way',
        () => {
            expect(
                itemIdentifier(
                    'table', 
                    {DeleteRequest: {Key: {foo: {B: '🐎👱❤'}}}}
                )
            ).toBe(
                itemIdentifier(
                    'table', 
                    {DeleteRequest: {Key: {foo: {B: Uint8Array.from([240, 159, 144, 142, 240, 159, 145, 177, 226, 157, 164])}}}}
                )
            );

            expect(
                itemIdentifier(
                    'table', 
                    {DeleteRequest: {Key: {foo: {B: '🐎👱❤'}}}}
                )
            ).toBe(
                itemIdentifier(
                    'table', 
                    {DeleteRequest: {Key: {foo: {B: Uint8Array.from([240, 159, 144, 142, 240, 159, 145, 177, 226, 157, 164]).buffer}}}}
                )
            );
        }
    );

    it('should throw when an invalid binary value is provided', () => {
        expect(
            () => itemIdentifier('table', {PutRequest: {Item: {foo: {B: []}}}})
        ).toThrow();
    });

    it(
        'should throw when neither a PutRequest nor a DeleteRequest is provided', 
        () => {
            expect(
                () => itemIdentifier('table', {} as any)
            ).toThrow();
        }
    );
});