import { DataSyncScheduler } from "../DataSyncScheduler";

export interface IDataProcessor {
  process(filename: string, data: string): Promise<void>;
  subscribe(scheduler: DataSyncScheduler): void;
}