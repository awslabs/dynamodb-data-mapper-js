import {PropertyAnnotation} from "./PropertyAnnotation";

export interface AttributeConfiguration {
    attributeName?: string;
}

export function attribute(
    configuration: AttributeConfiguration = {}
): PropertyAnnotation {
    return () => {
        console.log('foo');
    };
}