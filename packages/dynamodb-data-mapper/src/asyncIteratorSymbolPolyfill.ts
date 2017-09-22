if (Symbol && !Symbol.asyncIterator) {
    (Symbol as any).asyncIterator = "__@@asyncIterator__";
}
