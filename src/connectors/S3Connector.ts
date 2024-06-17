import { S3ClientMock } from "../mocks/S3ClientMock";
import { retryWithExponentialBackoff } from "../utils/retry";
import { IConnector } from "./IConnector";

// Use RateLimiter class to handle rate limit of service
export class S3Connector implements IConnector {
  constructor(
    private client = new S3ClientMock()
  ) { }

  public async getObject(bucket: string, key: string, offset: number, numBytes: number): Promise<string> {
    return retryWithExponentialBackoff<string>(
      async (): Promise<string> => {
        const res = await this.client.getObject(bucket, key, offset, numBytes);
        return res.buffer.subarray(0, res.bytesRead).toString();
      }, [bucket, key, offset, numBytes], 1);
  }

  public async getObjects(bucket: string): Promise<string[]> {
    // Get all files using paginated api
    return retryWithExponentialBackoff<string[]>(this.client.getObjects, [bucket], 1);
  }

  public async getObjectSize(bucket: string, key: string): Promise<number> {
    return retryWithExponentialBackoff<number>(this.client.getObjectSize, [bucket, key], 1);
  }
}