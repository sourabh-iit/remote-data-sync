export interface FileObject {
  readonly bytesRead: number;
  readonly buffer: Buffer;
}

export interface IConnector {
  getObject(bucket: string, key: string, offset: number, numBytes: number): Promise<string>;
  getObjectSize(bucket: string, key: string): Promise<number>;
  getObjects(bucket: string): Promise<string[]>;
}