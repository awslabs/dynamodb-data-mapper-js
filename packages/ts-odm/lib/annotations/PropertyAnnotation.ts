export interface PropertyAnnotation {
    (target: object, propertyKey: string|symbol): void;
}