export function isArrayOf<T>(
    arg: any,
    decider: (val: any) => val is T
): arg is Array<T> {
    if (!Array.isArray(arg)) {
        return false;
    }

    for (const el of arg) {
        if (!decider(el)) {
            return false;
        }
    }

    return true;
}
