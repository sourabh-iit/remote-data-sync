import * as fs from "fs";
import { FileObject } from "../connectors/IConnector";
import { sleep } from "../utils/common";

const LATENCY = 100;

export class S3ClientMock {
  public async getObjectSize(bucket: string, key: string): Promise<number> {
    return new Promise(async (res, rej) => {
      await sleep(LATENCY);
      fs.stat(`${bucket}/${key}`, (err, stats) => {
        if(err) {
          rej(err);
        } else {
          res(stats.size);
        }
      });
    });
  }

  public async getObject(bucket: string, key: string, offset: number, numBytes: number): Promise<FileObject> {
    return new Promise(async (res, rej) => {
      await sleep(LATENCY);
      fs.open(`${bucket}/${key}`, (err, fd) => {
        if(err) {
          rej(err);
        } else {
          fs.read(fd, Buffer.alloc(numBytes), 0, numBytes, offset, (err, bytesRead, buffer) => {
            if(err) {
              rej(err);
            } else {
              res({
                bytesRead,
                buffer
              });
            }
          });
        }
      });
    });
  }

  public async getObjects(bucket: string): Promise<string[]> {
    return new Promise(async (res, rej) => {
      await sleep(LATENCY);
      fs.readdir(bucket, (err, filenames) => {
        if(err) {
          rej(err);
        } else {
          res(filenames);
        }
      });
    });
  }
}