import {rangeKey} from './rangeKey';
import {v4} from 'uuid';

jest.mock('./attribute', () => ({attribute: jest.fn()}));
import {attribute} from './attribute';

describe('rangeKey', () => {
    beforeEach(() => {
        (attribute as any).mockClear();
    });

    it('should call attribute with a defined keyType', () => {
        const annotation = rangeKey();

        expect((attribute as any).mock.calls.length).toBe(1);
        expect((attribute as any).mock.calls[0]).toEqual([
            {keyType: 'RANGE'}
        ]);
    });

    it('should pass through any supplied parameters', () => {
        const attributeName = 'foo'
        const annotation = rangeKey({attributeName});

        expect((attribute as any).mock.calls[0][0])
            .toMatchObject({attributeName});
    });
});