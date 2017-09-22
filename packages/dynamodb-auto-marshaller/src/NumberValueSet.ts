import {ObjectSet} from "./ObjectSet";
import {NumberValue} from "./NumberValue";

/**
 * A set of numeric values represented internally as NumberValue objects.
 * Equality is determined by the string representation of the number and not by
 * the identity or data type of the provided value.
 */
export class NumberValueSet extends ObjectSet<NumberValue> {
    /**
     * @inheritDoc
     *
     * If a number or string is provided, it will be converted to a NumberValue
     * object.
     */
    add(value: NumberValue|number|string) {
        if (typeof value === 'number' || typeof value === 'string') {
            value = new NumberValue(value);
        }

        super.add(value);
        return this;
    }

    delete(value: NumberValue|number|string): boolean {
        const valueString = value.toString();
        const scrubbedValues = this._values
            .filter(item => item.toString() !== valueString);

        const numRemoved = this._values.length - scrubbedValues.length;
        this._values = scrubbedValues;

        return numRemoved > 0;
    }

    has(value: NumberValue|number|string): boolean {
        const valueString = value.toString();
        for (let item of this) {
            if (item.toString() === valueString) {
                return true;
            }
        }

        return false;
    }
}
