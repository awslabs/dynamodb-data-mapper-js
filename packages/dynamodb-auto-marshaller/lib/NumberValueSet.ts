import {ObjectSet} from "./ObjectSet";
import {NumberValue} from "./NumberValue";

export class NumberValueSet extends ObjectSet<NumberValue> {
    delete(value: NumberValue): boolean {
        const valueString = value.toString();
        const scrubbedValues = this._values
            .filter(item => item.toString() !== valueString);

        const numRemoved = this._values.length - scrubbedValues.length;
        this._values = scrubbedValues;

        return numRemoved > 0;
    }

    has(value: NumberValue): boolean {
        const valueString = value.toString();
        for (let item of this) {
            if (item.toString() === valueString) {
                return true;
            }
        }

        return false;
    }
}