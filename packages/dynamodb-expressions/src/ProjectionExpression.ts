import {AttributeName} from './AttributeName';
import {ExpressionAttributes} from './ExpressionAttributes';

export type ProjectionExpression = Array<AttributeName>;

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
