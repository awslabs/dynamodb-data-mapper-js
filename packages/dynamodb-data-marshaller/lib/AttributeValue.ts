import {AttributeValue as AttrVal} from 'aws-sdk/clients/dynamodb';

export type AttributeValue = AttrVal;

export function isAttributeValue(arg: any): arg is AttrVal {
    return Boolean(arg)
        && typeof arg === 'object'
        && (typeof arg.B === 'undefined' || isBinaryValue(arg.B))
        && (typeof arg.BS === 'undefined' || isSet(arg.BS, isBinaryValue))
        && ['boolean', 'undefined'].indexOf(typeof arg.BOOL) > 0
        && (typeof arg.NULL === 'undefined' || arg.NULL === true)
        && (typeof arg.N === 'undefined' || isNumericString(arg.N))
        && (typeof arg.NS === 'undefined' || isSet(arg.NS, isNumericString))
        && ['string', 'undefined'].indexOf(arg.S) > 0
        && (typeof arg.SS === 'undefined' || isSet(arg.SS, isString))

}

interface iface {
    /**
     * An attribute of type Map. For example:  "M": {"Name": {"S": "Joe"}, "Age": {"N": "35"}}
     */
    M ? : MapAttributeValue;
    /**
     * An attribute of type List. For example:  "L": ["Cookies", "Coffee", 3.14159]
     */
    L ? : ListAttributeValue;
}

function isArrayBuffer(arg: any): arg is ArrayBuffer {
    return (typeof ArrayBuffer === 'function' && arg instanceof ArrayBuffer) ||
        Object.prototype.toString.call(arg) === '[object ArrayBuffer]';
}

function isBinaryValue(arg: any): arg is ArrayBuffer|ArrayBufferView {
    return ArrayBuffer.isView(arg) || isArrayBuffer(arg);
}

type NumericString = string;

function isNumericString(arg: any): arg is NumericString {
    return typeof arg === 'string' && isFinite(Number(arg));
}

function isString(arg: any): arg is string {
    return typeof arg === 'string';
}

function isSet<T>(
    arg: any,
    decider: (val: any) => val is T
): arg is Set<T> {
    if (!isUntypedSet(arg)) {
        return false;
    }

    for (const el of arg) {
        if (!decider(el)) {
            return false;
        }
    }

    return true;
}

function isUntypedSet(arg: any): arg is Set<any> {
    return Boolean(arg)
        && Object.prototype.toString.call(arg) === '[object Set]';
}
