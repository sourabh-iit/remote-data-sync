export function retryWithExponentialBackoff<T>(fn: Function, args: any[], maxAttempts = 5, baseDelayMs = 1000) {
  let attempt = 1

  const execute = async (): Promise<T> => {
    try {
      return await fn(...args);
    } catch (error) {
      if (attempt >= maxAttempts) {
        throw error;
      }

      const delayMs = baseDelayMs * 2 ** attempt;
      console.log(`Retry attempt ${attempt} after ${delayMs}ms`);
      await new Promise((resolve) => setTimeout(resolve, delayMs));

      attempt++;
      return execute();
    }
  }

  return execute();
}