import { DataSyncScheduler } from "../DataSyncScheduler";
import { IDataProcessor } from "./IDataProcessor";
import * as fs from "fs";

export class DataProcessor implements IDataProcessor {
  private FILE_PATH = "data/files/";

  public subscribe(scheduler: DataSyncScheduler) {
    scheduler.subscribe(this);
  }

  public async process(filename: string, data: any): Promise<void> {
    if(!fs.existsSync(this.FILE_PATH)) {
      fs.mkdirSync(this.FILE_PATH);
    }
    
    return new Promise((resolve, reject) => {
      fs.open(`${this.FILE_PATH}/${filename}`, "a+", (err, fd) => {
        if(err) {
          reject(err);
        } else {
          fs.writeFileSync(fd, JSON.stringify(data) + "\n");
          fs.close(fd, (err) => {
            if(err) {
              reject(err);
            } else {
              resolve();
            }
          });
        }
      });
    });
  }
}