/**
 * Debounce utility for rate-limiting function calls.
 */

/**
 * Creates a debounced version of a function that delays invoking until
 * after `wait` milliseconds have elapsed since the last call.
 *
 * @param fn - The function to debounce
 * @param wait - The number of milliseconds to delay
 * @returns A debounced function with a `cancel` method
 */
export function debounce<T extends (...args: unknown[]) => void>(fn: T, wait: number): T & { cancel: () => void } {
  let timeoutId: ReturnType<typeof setTimeout> | null = null;

  const debounced = ((...args: Parameters<T>) => {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
    timeoutId = setTimeout(() => {
      fn(...args);
      timeoutId = null;
    }, wait);
  }) as T & { cancel: () => void };

  debounced.cancel = () => {
    if (timeoutId) {
      clearTimeout(timeoutId);
      timeoutId = null;
    }
  };

  return debounced;
}
