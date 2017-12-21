/**
 * @internal
 */
export function fromUtf8(input: string): Uint8Array {
    const bytes: Array<number> = [];
    for (let i = 0, len = input.length; i < len; i++) {
        const value = input.charCodeAt(i);
        if (value < 0x80) {
            bytes.push(value);
        } else if (value < 0x800) {
            bytes.push(
                (value >> 6) | 0b11000000,
                (value & 0b111111) | 0b10000000
            );
        } else if (
            i + 1 < input.length &&
            ((value & 0xfc00) === 0xd800) &&
            ((input.charCodeAt(i + 1) & 0xfc00) === 0xdc00)
        ) {
            const surrogatePair = 0x10000 +
                ((value & 0b1111111111) << 10) +
                (input.charCodeAt(++i) & 0b1111111111);
            bytes.push(
                (surrogatePair >> 18) | 0b11110000,
                ((surrogatePair >> 12) & 0b111111) | 0b10000000,
                ((surrogatePair >> 6) & 0b111111) | 0b10000000,
                (surrogatePair & 0b111111) | 0b10000000
            );
        } else {
            bytes.push(
                (value >> 12) | 0b11100000,
                ((value >> 6) & 0b111111) | 0b10000000,
                (value & 0b111111) | 0b10000000,
            );
        }
    }

    return Uint8Array.from(bytes);
}
