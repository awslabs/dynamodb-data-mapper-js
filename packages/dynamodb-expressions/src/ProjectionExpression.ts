import {AttributePath} from "./AttributePath";
import {ExpressionAttributes} from './ExpressionAttributes';

export type ProjectionExpression = Array<AttributePath|string>;

export function serializeProjectionExpression(
    projection: ProjectionExpression,
    attributes: ExpressionAttributes
): string {
    const serialized: Array<string> = [];
    for (const projected of projection) {
        serialized.push(attributes.addName(projected));
    }

    return serialized.join(', ');
}
