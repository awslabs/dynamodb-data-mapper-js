import {ObjectSet} from "./ObjectSet";

export type BinaryValue = Uint8Array;

/**
 * A set of binary values represented as either ArrayBuffer objects or
 * ArrayBufferView objects. Equality is determined by the underlying byte
 * sequence and not by the identity or view window type of the provided value.
 */
export class BinarySet extends ObjectSet<BinaryValue> {
    delete(value: BinaryValue): boolean {
        const valueView = getBinaryView(value);
        const scrubbedValues = this._values.filter(item => {
            return !binaryEquals(getBinaryView(item), valueView);
        });

        const numRemoved = this._values.length - scrubbedValues.length;
        this._values = scrubbedValues;

        return numRemoved > 0;
    }

    /**
     * @inheritDoc
     *
     * Equality is determined by inspecting the bytes of the ArrayBuffer or
     * ArrayBufferView.
     *
     * @example On a little-endian system, the following values would be
     * considered equal:
     *
     *     new Uint32Array([0xdeadbeef]);
     *     (new Uint32Array([0xdeadbeef])).buffer;
     *     new Uint16Array([0xbeef, 0xdead]);
     *     new Uint8Array([0xef, 0xbe, 0xad, 0xde]);
     */
    has(value: BinaryValue): boolean {
        const valueView = getBinaryView(value);

        for (let item of this) {
            if (binaryEquals(getBinaryView(item), valueView)) {
                return true;
            }
        }

        return false;
    }
}

function binaryEquals(a: DataView, b: DataView): boolean {
    if (a.byteLength !== b.byteLength) {
        return false;
    }

    for (let i = 0; i < a.byteLength; i++) {
        if (a.getUint8(i) !== b.getUint8(i)) {
            return false;
        }
    }

    return true;
}

function getBinaryView(value: BinaryValue): DataView {
    return ArrayBuffer.isView(value)
        ? new DataView(value.buffer, value.byteOffset, value.byteLength)
        : new DataView(value);
}
