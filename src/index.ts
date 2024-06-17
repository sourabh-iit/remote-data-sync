import { getEnvVariable } from "./conf";
import { DataSyncScheduler } from "./core/DataSyncScheduler";
import { DataProcessor } from "./core/processor/DataProcessor";
import * as cron from "node-cron";

cron.schedule(getEnvVariable("CRON_SCHEDULE"), async () => {
  const scheduler = DataSyncScheduler.getInstance();
  if(scheduler.isRunning) {
    console.log("Data sync job is already running");
    return;
  }
  const processor = new DataProcessor();
  processor.subscribe(scheduler);
  await scheduler.start();
});
