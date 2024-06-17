# Data sync scheduler
Develop a data synchronization scheduler that connects to Amazon S3, pulls data periodically,
generates local files per sync job, and handles failures by resuming from the last successful
checkpoint.

**Note: S3 client has been mocked using local file handler**

## Run project
Create image: `docker build . -t data-sync`

Run image: `docker run --env-file .env -v data:/home/app/data sync-job`


## Configure project
**CRON_SCHEDULE** - Pass a cron string to run scheduler at regular intervals of time. `Eg: 1 * * * * *`

**DATA_STORAGE_NAME** - Connector can be changed using this argument. `Eg: S3`

**BUCKET_NAME** - Folder path to assume as a bucket


## Run tests
`npm run test`