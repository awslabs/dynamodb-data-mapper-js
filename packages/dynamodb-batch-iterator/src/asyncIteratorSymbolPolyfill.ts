/**
 * Provides a simple polyfill for runtime environments that provide a Symbol
 * implementation but do not have Symbol.asyncIterator available by default.
 */

if (Symbol && !Symbol.asyncIterator) {
    (Symbol as any).asyncIterator = Symbol.for("__@@asyncIterator__");
}
