from healthcare_mlops.data.ingest import run_ingestion
from healthcare_mlops.data.validate import run_validation


def run_data_pipeline() -> None:
    """
    Full data pipeline:
      1. Ingestion (CSV -> Parquet)
      2. Validation (schema, types, ranges)
    """
    parquet_path = run_ingestion()
    run_validation(parquet_path)


if __name__ == "__main__":
    run_data_pipeline()
