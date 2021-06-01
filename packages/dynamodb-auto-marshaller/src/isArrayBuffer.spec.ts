import {isArrayBuffer} from "./isArrayBuffer";

describe('isArrayBuffer', () => {
    const arrayBufferConstructor = ArrayBuffer;

    afterEach(() => {
        (ArrayBuffer as any) = arrayBufferConstructor;
    });

    it('should return true for ArrayBuffer objects', () => {
        expect(isArrayBuffer(new ArrayBuffer(0))).toBe(true);
    });

    it('should return false for ArrayBufferView objects', () => {
        const view = new Uint8Array(0);

        expect(isArrayBuffer(view)).toBe(false);
        expect(isArrayBuffer(view.buffer)).toBe(true);
    });

    it('should return false for scalar values', () => {
        for (let scalar of ['string', 123.234, true, null, void 0]) {
            expect(isArrayBuffer(scalar)).toBe(false);
        }
    });
});
