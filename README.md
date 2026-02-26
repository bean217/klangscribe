# KlangScribe

Converting Song Audio to Playable Clone Hero Charts with Deep Learning.


## Environment Setup

### (1) klangscribe-orchestration subproject

Sync the `uv` environment

```bash
cd klangscribe-orchestration && uv sync
```
Note: `uv` must be installed

Create `klangscribe-orchestration/.env` with the following general configuration:

```bash
# General settings

# Dagster
SENSOR_BATCH_SIZE=20
```

* `SENSOR_BATCH_SIZE`
    * Batch size used by Dagster when performing data collection
    * If this value is too large, Dagster's sensor will timeout
    * A value of `20` works for remote services, and may be increased significantly for local


## Running as Local Dev vs. Cloud Mode

### (1) Local Development Mode

In order to mitigate issues with connecting to remote machines for external storage and services, we have provided a set of services (via Docker Compose) for running these locally.

Add the following to `klangscribe-orchestration/.env`:

```bash
# Local Environment Variables

# Dagster Postgres storage
DAGSTER_POSTGRES_HOST=localhost
DAGSTER_POSTGRES_PORT=5433
DAGSTER_POSTGRES_USER=dagster
DAGSTER_POSTGRES_PASSWORD=dagster
DAGSTER_POSTGRES_DB=dagster

# Klangscribe Postgres data storage
POSTGRES_HOST=localhost
POSTGRES_PORT=5432  # optional (default is set to 5432)
POSTGRES_USER=klangscribe
POSTGRES_PASSWORD=klangscribe
POSTGRES_DB=klangscribe

# Klangscribe S3/MinIO object storage
S3_ENDPOINT_URL=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_REGION=us-east-1

# Other settings
WATCH_DIR=/path/to/watch_dir
DATA_LIMIT=100
```

* `WATCH_DIR`
    * This is the directory path that Dagster watches for data collection
    * This must be set when working in local development mode, otherwise data collection will fail
* `DATA_LIMIT`
    * Used for testing purposes only
    * Enables materializing Dagster assets without needing to wait for all of the data to process at each step
    * **Not used when running inside a container**

When working in local development mode, it is strongly advised to run Dagster using the `dg dev` command:
```bash
cd klangscribe-orchestration
source .venv/bin/activate
dg dev
```

### (2) Cloud Mode

Recommended to use when remote storage services are available, and bulk or expensive read/write tasks are NOT needed.

Add the following to `klanscribe-orchestration/.env`:
```bash
# Local Environment Variables (uncomment for local development)

# Dagster Postgres storage
DAGSTER_POSTGRES_HOST=<dagster-postgres-database>
DAGSTER_POSTGRES_PORT=<dagster-postgres-port>
DAGSTER_POSTGRES_USER=<dagster-db-username>
DAGSTER_POSTGRES_PASSWORD=<dagster-db-password>
DAGSTER_POSTGRES_DB=<dagster-db-name>

# Klangscribe Postgres data storage
POSTGRES_HOST=<klangscribe-postgres-database>
POSTGRES_PORT=<klangscribe-postgres-port>
POSTGRES_USER=<klangscribe-postgres-port>
POSTGRES_PASSWORD=<klangscribe-db-username>
POSTGRES_DB=<klangscribe-db-name>

# Klangscribe S3/MinIO object storage
S3_ENDPOINT_URL=http://<s3-url>:<s3-port>   # must include `http://` prefix
S3_ACCESS_KEY=<s3-access-key>
S3_SECRET_KEY=<s3-secret-key>
S3_REGION=<s3-region>
```

When working in remote mode, Dagster services can be ran in dev or Container mode

(1) Dev Mode:
```bash
cd klangscribe-orchestration/docker
source .venv/bin/activate
dg dev
```

(2) Container Mode
```bash
cd klangscribe-orchestration/docker
docker compose up -d
```