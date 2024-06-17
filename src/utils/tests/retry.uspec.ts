import { retryWithExponentialBackoff } from "../retry";

jest.useFakeTimers();
jest.spyOn(global, 'setTimeout');

describe("retryWithExponentialBackoff", () => {
  const successValue = "success";
  const error = new Error("error");

  let mockFn: jest.Mock;

  beforeEach(() => {
    mockFn = jest.fn();
  });

  afterEach(() => {
    jest.clearAllMocks();
    jest.clearAllTimers();
  });

  it("should succeed on the first attempt", async () => {
    mockFn.mockResolvedValue(successValue);

    const result = await retryWithExponentialBackoff(mockFn, []);
    expect(result).toBe(successValue);
    expect(mockFn).toHaveBeenCalledTimes(1);
  });

  it("should retry and eventually succeed", async () => {
    mockFn
      .mockRejectedValueOnce(error)
      .mockRejectedValueOnce(error)
      .mockResolvedValue(successValue);

    const promise = retryWithExponentialBackoff(mockFn, []);
    
    for (let i = 1; i <= 2; i++) {
      jest.advanceTimersByTime(1000 * 2 ** i);
      await Promise.resolve();
    }

    const result = await promise;
    expect(result).toBe(successValue);
    expect(mockFn).toHaveBeenCalledTimes(3);
    expect(setTimeout).toHaveBeenCalledTimes(2);
  });

  it("should fail after max attempts", async () => {
    const maxAttempts = 3;
    mockFn.mockRejectedValue(error);

    const promise = retryWithExponentialBackoff(mockFn, [], maxAttempts);

    for (let i = 1; i < maxAttempts; i++) {
      jest.advanceTimersByTime(1000 * 2 ** i);
      await Promise.resolve();
    }

    await expect(promise).rejects.toThrow(error);
    expect(mockFn).toHaveBeenCalledTimes(maxAttempts);
    expect(setTimeout).toHaveBeenCalledTimes(maxAttempts - 1);
  });

  it("should respect custom base delay", async () => {
    const baseDelayMs = 500;
    mockFn
      .mockRejectedValueOnce(error)
      .mockResolvedValue(successValue);

    const promise = retryWithExponentialBackoff(mockFn, [], 5, baseDelayMs);

    jest.advanceTimersByTime(baseDelayMs * 2);
    await Promise.resolve();

    const result = await promise;
    expect(result).toBe(successValue);
    expect(mockFn).toHaveBeenCalledTimes(2);
    expect(setTimeout).toHaveBeenCalledWith(expect.any(Function), baseDelayMs * 2);
  });
});
