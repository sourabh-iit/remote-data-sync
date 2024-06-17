import { DataSyncScheduler } from "../DataSyncScheduler";
import { ConnectorFactory } from "../../connectors/ConnectorFactory";
import { IConnector } from "../../connectors/IConnector";
import { ICheckpointHandler } from "../../storage/ICheckpointHandler";
import { IDataProcessor } from "../processor/IDataProcessor";

jest.mock("../../connectors/ConnectorFactory");
jest.mock("../../storage/FileCheckpointHandler");

describe("DataSyncScheduler", () => {
  let mockConnector: jest.Mocked<IConnector>;
  let mockCheckpointHandler: jest.Mocked<ICheckpointHandler>;
  let scheduler: DataSyncScheduler;
  const OLD_ENV = process.env;

  beforeAll(() => {
    jest.resetModules();
    process.env.CRON_SCHEDULE = "MOCK_CRON_SCHEDULE";
    process.env.BUCKET_NAME = "MOCK_BUCKET_NAME";
    process.env.DATA_STORAGE_NAME = "MOCK_DATA_STORAGE_NAME";
  });

  beforeAll(() => {
    mockConnector = {
      getObjects: jest.fn(),
      getObject: jest.fn(),
      getObjectSize: jest.fn(),
    } as unknown as jest.Mocked<IConnector>;

    mockCheckpointHandler = {
      loadCheckpoint: jest.fn(),
      saveCheckpoint: jest.fn(),
    } as unknown as jest.Mocked<ICheckpointHandler>;

    (ConnectorFactory.getInstance as jest.Mock).mockReturnValue(mockConnector);
    scheduler = DataSyncScheduler.getInstance(mockCheckpointHandler);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  afterAll(() => {
    process.env = { ...OLD_ENV };
  });

  it("should start the sync job", async () => {
    const filenames = ["file1.txt", "file2.txt"];
    mockConnector.getObjects.mockResolvedValue(filenames);
    mockCheckpointHandler.loadCheckpoint.mockResolvedValue({});

    await scheduler.start();

    expect(mockConnector.getObjects).toHaveBeenCalledWith("MOCK_BUCKET_NAME");
    expect(mockCheckpointHandler.loadCheckpoint).toHaveBeenCalledWith(filenames);
  });

  it("should subscribe processors", () => {
    const mockProcessor: jest.Mocked<IDataProcessor> = {
      process: jest.fn(),
    } as unknown as jest.Mocked<IDataProcessor>;

    scheduler.subscribe(mockProcessor);
    expect((scheduler as any).subscribers).toContain(mockProcessor);
  });

  it("should process files correctly", async () => {
    const filename = "file1.txt";
    const filesize = 16;
    const chunk = '{"key":"value"}\n';

    mockConnector.getObjects.mockResolvedValue([filename]);
    mockCheckpointHandler.loadCheckpoint.mockResolvedValue({});
    mockConnector.getObjectSize.mockResolvedValue(filesize);
    mockConnector.getObject.mockResolvedValue(chunk);

    const mockProcessor: jest.Mocked<IDataProcessor> = {
      process: jest.fn(),
    } as unknown as jest.Mocked<IDataProcessor>;

    scheduler.subscribe(mockProcessor);

    await scheduler.start();

    expect(mockConnector.getObjectSize).toHaveBeenCalledWith("MOCK_BUCKET_NAME", filename);
    expect(mockConnector.getObject).toHaveBeenCalledTimes(1);
    expect(mockConnector.getObject).toHaveBeenNthCalledWith(1, "MOCK_BUCKET_NAME", filename, 0, 32);
    expect(mockProcessor.process).toHaveBeenCalledWith(filename, { key: "value" });
  });

  it("should handle checkpointing correctly", async () => {
    const filename = "file1.txt";
    const filesize = 16;
    const chunk = '{"key":"value"}\n';
    const checkpointData = { [filename]: { bytesRead: -1, isProcessed: false } };

    mockConnector.getObjects.mockResolvedValue([filename]);
    mockCheckpointHandler.loadCheckpoint.mockResolvedValue(checkpointData);
    mockConnector.getObjectSize.mockResolvedValue(filesize);
    mockConnector.getObject.mockResolvedValue(chunk);

    const mockProcessor: jest.Mocked<IDataProcessor> = {
      process: jest.fn(),
    } as unknown as jest.Mocked<IDataProcessor>;

    scheduler.subscribe(mockProcessor);

    await scheduler.start();

    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenCalledTimes(2);
    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenNthCalledWith(1, filename, chunk.length-1);
    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenNthCalledWith(2, filename, chunk.length-1, true);
  });

  it("should handle case when all json come in same chunk", async () => {
    const filename = "file1.txt";
    const chunk = '{"k1":"v1"}\n{"k2":"v2"}\n';
    const filesize = chunk.length;
    const checkpointData = { [filename]: { bytesRead: -1, isProcessed: false } };

    mockConnector.getObjects.mockResolvedValue([filename]);
    mockCheckpointHandler.loadCheckpoint.mockResolvedValue(checkpointData);
    mockConnector.getObjectSize.mockResolvedValue(filesize);
    mockConnector.getObject.mockResolvedValue(chunk);

    const mockProcessor: jest.Mocked<IDataProcessor> = {
      process: jest.fn(),
    } as unknown as jest.Mocked<IDataProcessor>;

    scheduler.subscribe(mockProcessor);

    await scheduler.start();

    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenCalledTimes(3);
    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenNthCalledWith(1, filename, 11);
    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenNthCalledWith(2, filename, 23);
    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenNthCalledWith(3, filename, 23, true);
    expect(mockProcessor.process).toHaveBeenCalledTimes(2);
    expect(mockProcessor.process).toHaveBeenNthCalledWith(1, filename, {"k1":"v1"});
    expect(mockProcessor.process).toHaveBeenNthCalledWith(2, filename, {"k2":"v2"});
  });

  it("should handle case when \n is read in next chunk", async () => {
    const filename = "file1.txt";
    const chunk = '{"key":"value12345678912345678"}\n{"key":"value1234567891234567832531"}';
    const filesize = chunk.length;
    const checkpointData = { [filename]: { bytesRead: -1, isProcessed: false } };

    mockConnector.getObjects.mockResolvedValue([filename]);
    mockCheckpointHandler.loadCheckpoint.mockResolvedValue(checkpointData);
    mockConnector.getObjectSize.mockResolvedValue(filesize);
    mockConnector.getObject.mockResolvedValue(chunk);

    const mockProcessor: jest.Mocked<IDataProcessor> = {
      process: jest.fn(),
    } as unknown as jest.Mocked<IDataProcessor>;

    scheduler.subscribe(mockProcessor);

    await scheduler.start();

    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenCalledTimes(2);
    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenNthCalledWith(1, filename, 32);
    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenNthCalledWith(2, filename, chunk.length-1, true);
    expect(mockProcessor.process).toHaveBeenCalledTimes(2);
    expect(mockProcessor.process).toHaveBeenNthCalledWith(1, filename, {"key":"value12345678912345678"});
    expect(mockProcessor.process).toHaveBeenNthCalledWith(2, filename, {"key":"value1234567891234567832531"});
  });

  it("should handle multiple lines correctly", async () => {
    const filename = "file1.txt";
    const chunk = '{"key1":"value1"}\n{"key2":"value2"}\n{"key3":"value3"}\n{"key4":"value4"}\n{"key5":"value5"}\n';
    const filesize = chunk.length;
    const checkpointData = { [filename]: { bytesRead: -1, isProcessed: false } };

    mockConnector.getObjects.mockResolvedValue([filename]);
    mockCheckpointHandler.loadCheckpoint.mockResolvedValue(checkpointData);
    mockConnector.getObjectSize.mockResolvedValue(filesize);
    mockConnector.getObject.mockResolvedValue(chunk);

    const mockProcessor: jest.Mocked<IDataProcessor> = {
      process: jest.fn(),
    } as unknown as jest.Mocked<IDataProcessor>;

    scheduler.subscribe(mockProcessor);

    await scheduler.start();

    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenCalledTimes(6);
    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenNthCalledWith(1, filename, 17);
    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenNthCalledWith(2, filename, 35);
    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenNthCalledWith(3, filename, 53);
    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenNthCalledWith(4, filename, 71);
    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenNthCalledWith(5, filename, 89);
    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenNthCalledWith(6, filename, 89, true);
    expect(mockProcessor.process).toHaveBeenCalledTimes(5);
    expect(mockProcessor.process).toHaveBeenNthCalledWith(1, filename, { key1: "value1" });
    expect(mockProcessor.process).toHaveBeenNthCalledWith(2, filename, { key2: "value2" });
    expect(mockProcessor.process).toHaveBeenNthCalledWith(3, filename, { key3: "value3" });
    expect(mockProcessor.process).toHaveBeenNthCalledWith(4, filename, { key4: "value4" });
    expect(mockProcessor.process).toHaveBeenNthCalledWith(5, filename, { key5: "value5" });
  });

  it("should handle correctly when there is no line break on last line", async () => {
    const filename = "file1.txt";
    const chunk = '{"key":"value"}';
    const filesize = chunk.length;
    const checkpointData = { [filename]: { bytesRead: -1, isProcessed: false } };

    mockConnector.getObjects.mockResolvedValue([filename]);
    mockCheckpointHandler.loadCheckpoint.mockResolvedValue(checkpointData);
    mockConnector.getObjectSize.mockResolvedValue(filesize);
    mockConnector.getObject.mockResolvedValue(chunk);

    const mockProcessor: jest.Mocked<IDataProcessor> = {
      process: jest.fn(),
    } as unknown as jest.Mocked<IDataProcessor>;

    scheduler.subscribe(mockProcessor);

    await scheduler.start();

    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenCalledTimes(1);
    expect(mockCheckpointHandler.saveCheckpoint).toHaveBeenNthCalledWith(1, filename, chunk.length-1, true);
    expect(mockProcessor.process).toHaveBeenCalledWith(filename, { key: "value" });
  });
});
