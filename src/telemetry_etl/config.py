from dataclasses import dataclass
import os

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    aws_region: str
    s3_bucket: str
    s3_raw_prefix: str
    s3_processed_prefix: str
    snowflake_account: str
    snowflake_user: str
    snowflake_password: str
    snowflake_role: str
    snowflake_warehouse: str
    snowflake_database: str
    snowflake_schema: str


def get_settings() -> Settings:
    return Settings(
        aws_region=os.getenv("AWS_REGION", "ap-south-1"),
        s3_bucket=os.getenv("S3_BUCKET", "tesla-vehicle-telemetry-shyam"),
        s3_raw_prefix=os.getenv("S3_RAW_PREFIX", "raw/telemetry"),
        s3_processed_prefix=os.getenv("S3_PROCESSED_PREFIX", "processed/telemetry"),
        snowflake_account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
        snowflake_user=os.getenv("SNOWFLAKE_USER", ""),
        snowflake_password=os.getenv("SNOWFLAKE_PASSWORD", ""),
        snowflake_role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        snowflake_warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "TELEMETRY_WH"),
        snowflake_database=os.getenv("SNOWFLAKE_DATABASE", "TESLA_TELEMETRY"),
        snowflake_schema=os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
    )

