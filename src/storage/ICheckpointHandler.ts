export interface FileCheckpointData {
  readonly isProcessed: boolean;
  readonly bytesRead: number;
}

export interface CheckpointData {
  [filename: string]: FileCheckpointData;
}

export interface ICheckpointHandler {
  loadCheckpoint(filenames: string[]): Promise<CheckpointData>;
  saveCheckpoint(filename: string, bytesRead: number, isProcessed?: boolean): Promise<void>;
}