from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task, get_current_context

from telemetry_etl.extract import download_s3_object, list_s3_objects
from telemetry_etl.load import fetch_processed_s3_keys, load_curated_outputs, mark_s3_objects_processed
from telemetry_etl.quality import assert_quality, run_quality_checks
from telemetry_etl.transform import build_vehicle_hourly_metrics, enrich_telemetry, read_jsonl


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

LANDING_DIR = Path("/opt/airflow/build/landing")
CURATED_DIR = Path("/opt/airflow/build/curated")


@dag(
    dag_id="tesla_vehicle_telemetry_etl",
    description="Process Tesla telemetry from S3 into Snowflake monitoring models.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["tesla", "telemetry", "s3", "snowflake"],
)
def tesla_vehicle_telemetry_etl():
    @task
    def discover_raw_objects() -> list[str]:
        keys = list_s3_objects()
        processed_keys = fetch_processed_s3_keys()
        return [key for key in keys if key.endswith(".jsonl") and key not in processed_keys]

    @task
    def extract_raw_files(keys: list[str]) -> list[str]:
        if not keys:
            return []
        return [download_s3_object(key, LANDING_DIR) for key in keys]

    @task
    def validate_raw_files(paths: list[str]) -> list[str]:
        if not paths:
            return []
        for path in paths:
            df = read_jsonl(path)
            assert_quality(run_quality_checks(df))
        return paths

    @task
    def transform_raw_files(paths: list[str]) -> dict[str, str]:
        if not paths:
            return {}

        CURATED_DIR.mkdir(parents=True, exist_ok=True)
        enriched_frames = []
        metrics_frames = []
        for path in paths:
            raw = read_jsonl(path)
            enriched = enrich_telemetry(raw)
            enriched_frames.append(enriched)
            metrics_frames.append(build_vehicle_hourly_metrics(enriched))

        import pandas as pd

        enriched_path = CURATED_DIR / "telemetry_enriched.parquet"
        metrics_path = CURATED_DIR / "vehicle_hourly_metrics.parquet"
        pd.concat(enriched_frames, ignore_index=True).to_parquet(enriched_path, index=False)
        pd.concat(metrics_frames, ignore_index=True).to_parquet(metrics_path, index=False)
        return {
            "telemetry_enriched": str(enriched_path),
            "vehicle_hourly_metrics": str(metrics_path),
        }

    @task
    def load_curated_to_snowflake(outputs: dict[str, str]) -> None:
        if not outputs:
            return
        context = get_current_context()
        stage_prefix = f"airflow/{context['run_id'].replace(':', '_')}"
        load_curated_outputs(outputs, stage_prefix)

    @task
    def record_processed_objects(keys: list[str]) -> None:
        if not keys:
            return
        context = get_current_context()
        mark_s3_objects_processed(keys, context["run_id"])

    raw_keys = discover_raw_objects()
    downloaded = extract_raw_files(raw_keys)
    valid_paths = validate_raw_files(downloaded)
    curated_outputs = transform_raw_files(valid_paths)
    load_task = load_curated_to_snowflake(curated_outputs)
    record_task = record_processed_objects(raw_keys)
    load_task >> record_task


tesla_vehicle_telemetry_etl()
