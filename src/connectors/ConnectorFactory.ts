import { IConnector } from "./IConnector";
import { S3Connector } from "./S3Connector";

export class ConnectorFactory {
  private static s3Connector = new S3Connector();

  static getInstance(name: string): IConnector {
    if (name === "S3") {
      return this.s3Connector;
    }
    throw new Error(`Invalid connector name ${name}`);
  }
}