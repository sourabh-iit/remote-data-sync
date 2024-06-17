import { ConnectorFactory } from "../connectors/ConnectorFactory";
import { IConnector } from "../connectors/IConnector";
import { FileCheckpointHandler } from "../storage/FileCheckpointHandler";
import { CheckpointData, ICheckpointHandler } from "../storage/ICheckpointHandler";
import { CHUNK_SIZE, getEnvVariable } from "../conf";
import { IDataProcessor } from "./processor/IDataProcessor";

export class DataSyncScheduler {
  public isRunning = false;
  private connector: IConnector;
  private checkpointData: CheckpointData;
  private subscribers: IDataProcessor[] = [];
  private static instance: DataSyncScheduler;
  private LINE_BREAK = "\n";

  private constructor(
    // Using file to save checkpoints but can be modified to key value store by changing handler
    private checkpointHandler: ICheckpointHandler
  ) {
    // Use factory pattern to access connector
    this.connector = ConnectorFactory.getInstance(getEnvVariable("DATA_STORAGE_NAME"));
  }

  // Create singleton
  static getInstance(checkpointHandler: ICheckpointHandler = new FileCheckpointHandler()) {
    if(!this.instance) {
      this.instance = new this(checkpointHandler);
    }
    return this.instance;
  }

  // Use observer pattern to decouple action from sync
  public subscribe(processor: IDataProcessor) {
    this.subscribers.push(processor);
  }

  private async publish(filename: string, data: any) {
    for(const subscriber of this.subscribers) {
      await subscriber.process(filename, data);
    }
  }

  public async start() {
    console.log("Sync job started");
    this.isRunning = true;
    // In real S3 storage, we can add time in checkpoint and then can load files after that time. 
    // In this way, we can avoid loading old files
    const filenames = await this.connector.getObjects(getEnvVariable("BUCKET_NAME"));
    this.checkpointData = await this.checkpointHandler.loadCheckpoint(filenames);
    for(const filename of filenames) {
      // Serialize loading as one file can be dependent on previous files data
      await this.processFile(filename);
    }
    this.isRunning = false;
    console.log("Sync job finished");
  }

  private async processFile(filename: string) {
    if(this.checkpointData[filename]?.isProcessed) {
      return;
    }

    console.log(`Syncing file ${filename}`);

    const filesize = await this.connector.getObjectSize(getEnvVariable("BUCKET_NAME"), filename);

    let bytesRead = this.checkpointData[filename]?.bytesRead ?? -1;
    let lineData = [];
    while(bytesRead + 1 < filesize) {
      console.log("bytes", bytesRead);
      const chunk = await this.connector.getObject(getEnvVariable("BUCKET_NAME"), filename, bytesRead + 1, CHUNK_SIZE);
      for(let i = 0; i < chunk.length; i++) {
        bytesRead += 1;
        if(chunk[i] == this.LINE_BREAK) {
          const data = lineData.join("").trim();
          await this.publish(filename, JSON.parse(data));
          await this.checkpointHandler.saveCheckpoint(filename, bytesRead);
          lineData = [];
        } else {
          lineData.push(chunk[i]);
        }
      }
    }

    // handle case when there is no \n in last line
    if(lineData.join().trim().length > 0) {
      const data = JSON.parse(lineData.join(""));
      await this.publish(filename, data);
    }

    // Mark current file as fully processed. This can save api call for file size
    await this.checkpointHandler.saveCheckpoint(filename, bytesRead, true);
  }
}