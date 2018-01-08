import { versionAttribute } from './versionAttribute';

jest.mock('./attribute', () => ({attribute: jest.fn()}));
import { attribute } from './attribute';

describe('versionAttribute', () => {
    beforeEach(() => {
        (attribute as any).mockClear();
    });

    it(
        'should call attribute with a defined type and versionAttribute trait',
        () => {
            versionAttribute();

            expect((attribute as any).mock.calls.length).toBe(1);
            expect((attribute as any).mock.calls[0]).toEqual([
                {
                    type: 'Number',
                    versionAttribute: true,
                }
            ]);
        }
    );

    it('should pass through any supplied parameters', () => {
        const attributeName = 'foo'
        versionAttribute({attributeName});

        expect((attribute as any).mock.calls[0][0])
            .toMatchObject({attributeName});
    });
});
