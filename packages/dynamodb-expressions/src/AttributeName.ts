export type ScalarAttributeName = string;

export interface ListIndexAttributeName {
    listAttributeName: AttributeName;
    index: number;
}

export interface MapPropertyAttributeName {
    mapAttributeName: AttributeName;
    propertyAttributeName: AttributeName;
}

export type AttributeName =
    ScalarAttributeName |
    ListIndexAttributeName |
    MapPropertyAttributeName;

export function isAttributeName(arg: any): arg is AttributeName {
    return isScalarAttributeName(arg)
        || isListIndexAttributeName(arg)
        || isMapPropertyAttributeName(arg);
}

export function isScalarAttributeName(arg: any): arg is ScalarAttributeName {
    return typeof arg === 'string';
}

export function isListIndexAttributeName(
    arg: any
): arg is ListIndexAttributeName {
    return Boolean(arg)
        && typeof arg === 'object'
        && typeof arg.index === 'number'
        && isAttributeName(arg.listAttributeName);
}

export function isMapPropertyAttributeName(
    arg: any
): arg is MapPropertyAttributeName {
    return Boolean(arg)
        && typeof arg === 'object'
        && isAttributeName(arg.propertyAttributeName)
        && isAttributeName(arg.mapAttributeName);
}
