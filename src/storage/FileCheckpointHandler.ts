import { CheckpointData, ICheckpointHandler } from "./ICheckpointHandler";
import * as fs from "fs"; 

export class FileCheckpointHandler implements ICheckpointHandler {
  private dataFilename = "data/checkpoint.json";

  public async loadCheckpoint(filenames: string[] = []): Promise<CheckpointData> {
    try {
      const data = fs.readFileSync(this.dataFilename);
      return JSON.parse(data.toString());
    } catch(err) {
      return {};
    }
  }

  public async saveCheckpoint(filename: string, bytesRead: number, isProcessed = false) {
    const data = await this.loadCheckpoint();
    data[filename] = {
      bytesRead,
      isProcessed
    }
    fs.writeFileSync(this.dataFilename, JSON.stringify(data));
  }
}