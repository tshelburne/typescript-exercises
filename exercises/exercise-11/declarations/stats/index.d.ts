declare module 'stats' {
	type Comparator<T> = (el1: T, el2: T) => number

    export function getMaxIndex<T>(input: T[], comparator: Comparator<T>): number;
    export function getMaxElement<T>(input: T[], comparator: Comparator<T>): T;
    export function getMinIndex<T>(input: T[], comparator: Comparator<T>): number;
    export function getMinElement<T>(input: T[], comparator: Comparator<T>): T;
    export function getMedianIndex<T>(input: T[], comparator: Comparator<T>): number;
    export function getMedianElement<T>(input: T[], comparator: Comparator<T>): T;
    export function getAverageValue<T>(input: T[], getValue: (el: T) => number): number;
}
