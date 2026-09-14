# Tesla Vehicle Telemetry ETL Pipeline

A production-style end-to-end data engineering pipeline for processing Tesla-style connected vehicle telemetry using **Amazon S3, Apache Airflow, Python, and Snowflake**.

This project demonstrates cloud-based ingestion, incremental processing, data quality validation, transformation, Snowflake loading, orchestration, testing, and CI/CD practices.

---

## 📌 Project Overview

Connected vehicles continuously generate telemetry such as:

- Vehicle identification
- GPS location
- Speed
- Battery percentage
- Odometer readings
- Power consumption
- Charging state
- Tire pressure
- Outside temperature
- Event timestamps

This project simulates that environment by ingesting telemetry JSONL files from Amazon S3, validating and transforming the data using Python, and loading curated datasets into Snowflake.

The pipeline is orchestrated using Apache Airflow running locally through Docker Compose.

---

# 🏗️ Architecture

```mermaid
flowchart LR

    A["Tesla-style Telemetry JSONL"] --> B["Amazon S3<br/>Raw Zone"]

    B --> C["Apache Airflow<br/>ETL DAG"]

    C --> D["Discover Raw Objects"]

    D --> E["Extract"]

    E --> F["Validate"]

    F --> G["Transform"]

    G --> H["Curated Parquet"]

    H --> I["Snowflake Internal Stage"]

    I --> J["Snowflake Staging Tables"]

    J --> K["Curated Analytics Tables"]

    C --> L["Processed S3 Object Tracking"]

    L --> M["Incremental Processing"]

---

# 🔧 Technology Stack
| Technology              | Purpose                                        |
| ----------------------- | ---------------------------------------------- |
| Python                  | ETL processing, validation and transformations |
| Apache Airflow          | Workflow orchestration                         |
| Docker / Docker Compose | Local Airflow environment                      |
| Amazon S3               | Raw and processed data storage                 |
| Snowflake               | Analytical data warehouse                      |
| Pandas / PyArrow        | Data transformation and Parquet generation     |
| Pytest                  | Unit testing                                   |
| GitHub Actions          | Continuous Integration                         |
| PowerShell              | Windows development and execution              |

---

# 🚀 Pipeline Flow

The Airflow DAG contains the following major steps:

discover_raw_objects
        ↓
extract_raw_files
        ↓
validate_raw_files
        ↓
transform_raw_files
        ↓
load_curated_to_snowflake
        ↓
record_processed_objects

1. Discover Raw Objects
The pipeline scans the configured S3 raw telemetry location. Example: s3://<bucket>/raw/telemetry/

2. Incremental Processing
The pipeline does not blindly process every file on every run.Processed S3 objects are tracked using a Snowflake control table: Example: PROCESSED_S3_OBJECTS
This allows the pipeline to determine which S3 objects have already been successfully processed.
Conceptually:
S3 Object
   ↓
Already processed?
   ├── YES → Skip
   └── NO  → Process

3. Extract
Raw JSONL telemetry files are downloaded/read from S3 and prepared for processing. The sample telemetry format contains fields such as:
{
  "event_id": "evt-1001",
  "vin": "5YJ3E1EA7KF317001",
  "vehicle_model": "Model 3",
  "event_ts": "2026-05-25T08:00:00Z",
  "latitude": 37.7749,
  "longitude": -122.4194,
  "speed_mph": 42.5,
  "battery_pct": 78.2,
  "odometer_miles": 18432.1,
  "power_kw": -18.4,
  "charging_state": "driving"
}

---
#✅ Data Quality Validation
Before transformation and loading, telemetry records are validated.

The project includes checks for:

- Required fields
- VIN validity
- Duplicate event IDs
- Battery percentage range
- Speed range
- Latitude and longitude
- Odometer values
- Timestamp validity
- Required telemetry attributes

Invalid records are prevented from silently flowing into the analytical layer.

---
# 🔄 Transformation
Validated telemetry is transformed into analytical datasets.

The transformation layer produces enriched telemetry and monitoring-oriented metrics.

Examples include:

- Vehicle telemetry enrichment
- Battery health information
- Driving metrics
- Charging information
- Vehicle monitoring metrics
- Alert-oriented attributes
- Hourly vehicle metrics

The transformed data is written as Parquet files.
Example output:
build/curated/
├── telemetry_enriched.parquet
└── vehicle_hourly_metrics.parquet

---
# ❄️ Snowflake Loading
The curated Parquet files are loaded into Snowflake.

The loading process follows a staging architecture:
Curated Parquet
      ↓
Snowflake Internal Stage
      ↓
Staging Tables
      ↓
COPY INTO
      ↓
MERGE
      ↓
Curated Analytics Tables

The Snowflake setup is separated into SQL scripts.
snowflake/
├── 001_create_database_schema.sql
├── 002_create_tables.sql
└── 003_curated_models.sql
Run these scripts in order.

---
# 📊 Snowflake Data Model
The project separates ingestion/control data from analytical data.

The processed-object control mechanism tracks successfully processed S3 objects.

The analytical layer contains curated telemetry and monitoring datasets intended for downstream reporting and dashboards.

---
# 🧪 Sample Data
The repository contains sample JSONL files for local development and incremental-processing testing.
data/
└── sample/
    ├── tesla_telemetry_sample.jsonl
    ├── tesla_telemetry_incremental.jsonl
    └── tesla_telemetry_new.jsonl
The sample files allow the complete pipeline to be tested without requiring a real Tesla telemetry source.

---
# 🐳 Running Airflow Locally
The project uses Docker Compose to run Apache Airflow locally.

Initialize Airflow:
      docker compose up airflow-init

Start the services:
      docker compose up

Airflow UI:
      http://localhost:8080

The local Airflow credentials are configured through the Docker Compose configuration.

Do not commit real credentials or secrets to GitHub.

---
# ⚙️ Environment Configuration
Create a local .env file from the example:
      Copy-Item .env.example .env

Then configure the required values.
Example:
      AWS_REGION=ap-south-1
      S3_BUCKET=<your-s3-bucket>
      S3_RAW_PREFIX=raw/telemetry
      S3_PROCESSED_PREFIX=processed/telemetry

      SNOWFLAKE_ACCOUNT=<your-snowflake-account>
      SNOWFLAKE_USER=<your-snowflake-user>
      SNOWFLAKE_PASSWORD=<your-snowflake-password>
      SNOWFLAKE_ROLE=<your-snowflake-role>
      SNOWFLAKE_WAREHOUSE=TELEMETRY_WH
      SNOWFLAKE_DATABASE=TESLA_TELEMETRY
      SNOWFLAKE_SCHEMA=ANALYTICS
The actual .env file is intentionally excluded from Git using .gitignore

---
# 🧑‍💻 Local Python Development
Create a virtual environment:
      python -m venv .venv
Activate it:
      .\.venv\Scripts\Activate.ps1
Install dependencies:
      pip install -e ".[dev]"

---
# 🧪 Running Tests
Run the complete test suite:
      pytest

The repository contains tests covering transformation and data-quality functionality.

Test files:

      tests/
├── test_quality.py
└── test_transform.py

===
# ▶️ Running the Local ETL

The project includes a PowerShell helper script:
      .\scripts\run_local_etl.ps1

The transformation can also be executed directly.

Example:
      python -m telemetry_etl.transform data/sample/tesla_telemetry_sample.jsonl build/curated

---
# ☁️ Uploading Sample Data to S3
Example:
      aws s3 cp `
      .\data\sample\tesla_telemetry_sample.jsonl `
      s3://<your-bucket>/raw/telemetry/tesla_telemetry_sample.jsonl `
      --region ap-south-1

Verify:
      aws s3 ls s3://<your-bucket>/raw/telemetry/ --region ap-south-1

---
# ▶️ Triggering the Airflow DAG

The DAG ID is:
      tesla_vehicle_telemetry_etl

Trigger manually:
      docker compose exec airflow-scheduler airflow dags trigger tesla_vehicle_telemetry_etl

Check DAG runs:
      docker compose exec airflow-scheduler airflow dags list-runs tesla_vehicle_telemetry_etl

Check the tasks for a particular run:
      docker compose exec airflow-scheduler airflow tasks states-for-dag-run tesla_vehicle_telemetry_etl "<RUN_ID>"

---
# 📈 Pipeline Validation Performed
During development, the pipeline was tested with incremental sample files.
The processing counts were validated across multiple runs.
For example, the sample progression demonstrated:
Initial processing:
5 records

Incremental processing:
3 records

New processing:
2 records

The pipeline successfully completed the Airflow tasks for the validated runs.
This confirmed that the discovery, extraction, validation, transformation, Snowflake loading, and processed-object tracking workflow was functioning as expected.

---
# 🛠️ Problems Encountered and Resolved
Building the project involved several development and debugging iterations.
1. Airflow DAG execution issues
Earlier DAG runs failed during development.
The pipeline was investigated using Airflow CLI commands such as:
      docker compose exec airflow-scheduler airflow dags list-runs tesla_vehicle_telemetry_etl

and:
      docker compose exec airflow-scheduler airflow tasks states-for-dag-run tesla_vehicle_telemetry_etl "<RUN_ID>"

This allowed individual task failures to be isolated.

The final validated runs completed successfully.


2. Airflow API/JWT configuration

The local Airflow Docker configuration required compatible API authentication settings.

The Docker Compose configuration was updated to provide the required JWT secret configuration.

For security, the repository does not contain a real production secret.


3. Environment configuration
The original project configuration was adapted for the local AWS/Snowflake environment.

The configuration was moved toward environment-based settings instead of hard-coding credentials.

The repository contains:
      .env.example
while the actual:
      .env

4. AWS S3 configuration
The project was configured to use an S3 bucket in:
      ap-south-1
The raw telemetry location follows:
      raw/telemetry/
This allowed the Airflow pipeline to discover and process uploaded telemetry objects.

5. Incremental ingestion testing
Additional telemetry files were introduced specifically to verify incremental processing.

      tesla_telemetry_sample.jsonl
      tesla_telemetry_incremental.jsonl
      tesla_telemetry_new.jsonl

This helped validate that the pipeline could process newly discovered objects without unnecessarily reprocessing previously completed objects.

---
# 🔍 Observability and Debugging
Airflow CLI commands were used extensively during development.
List DAG runs:
      docker compose exec airflow-scheduler airflow dags list-runs tesla_vehicle_telemetry_etl

Inspect task states:
      docker compose exec airflow-scheduler airflow tasks states-for-dag-run tesla_vehicle_telemetry_etl "<RUN_ID>"

The task-level execution history provides visibility into:
      discover_raw_objects
      extract_raw_files
      validate_raw_files
      transform_raw_files
      load_curated_to_snowflake
      record_processed_objects
This makes it easier to identify exactly where an ETL execution fails.

---
#📁 Repository Structure
Tesla-Vehicle-Telemetry-ETL-Pipeline/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── dags/
│   └── tesla_telemetry_etl_dag.py
│
├── data/
│   └── sample/
│       ├── tesla_telemetry_sample.jsonl
│       ├── tesla_telemetry_incremental.jsonl
│       └── tesla_telemetry_new.jsonl
│
├── docs/
│   ├── architecture.md
│   └── data_dictionary.md
│
├── scripts/
│   └── run_local_etl.ps1
│
├── snowflake/
│   ├── 001_create_database_schema.sql
│   ├── 002_create_tables.sql
│   └── 003_curated_models.sql
│
├── src/
│   └── telemetry_etl/
│       ├── config.py
│       ├── extract.py
│       ├── load.py
│       ├── quality.py
│       ├── schemas.py
│       └── transform.py
│
├── tests/
│   ├── test_quality.py
│   └── test_transform.py
│
├── docker-compose.yml
├── pyproject.toml
├── requirements.txt
├── Makefile
├── LICENSE
├── .env.example
└── README.md

---
#🔐 Security
Sensitive configuration must never be committed to GitHub.

The following file is intentionally ignored:
      .env

Use:
      .env.example

as the configuration template.

Never commit:

      AWS access keys
      AWS secret keys
      Snowflake passwords
      Private keys
      API tokens
      JWT secrets
      Other production credentials

---
# 🎯 Key Data Engineering Concepts Demonstrated
This project demonstrates practical implementation of:

      ETL / ELT architecture
      Cloud object storage
      Incremental ingestion
      Idempotent processing
      Data quality validation
      Schema enforcement
      Apache Airflow orchestration
      Python-based ETL
      Parquet data processing
      Snowflake staging
      Snowflake COPY INTO
      Snowflake MERGE
      Control tables
      Analytical data modeling
      Unit testing
      Docker
      Environment-based configuration
      CI/CD
      Pipeline observability
      Failure debugging
      Cloud-to-warehouse data movement

---
# 📌 Project Status
Status: Completed and validated locally

The pipeline has been tested through:

      Local Python execution
      Sample JSONL telemetry
      S3 ingestion
      Incremental sample files
      Airflow DAG execution
      Airflow task-level validation
      Snowflake loading
      Data quality validation
      Automated tests
      GitHub repository deployment

The latest validated Airflow runs completed successfully.


