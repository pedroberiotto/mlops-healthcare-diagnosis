from pathlib import Path

import pandas as pd

from healthcare_mlops.config import paths


def load_raw_data(path: Path | None = None) -> pd.DataFrame:
    """
    Read the raw CSV file and return a DataFrame with basic type casting.
    """
    csv_path = path or paths.raw_csv
    if not csv_path.exists():
        raise FileNotFoundError(f"Raw data not found at {csv_path}")

    df = pd.read_csv(csv_path)

    # Basic type casting; adjust column names if your CSV differs
    df["Patient_ID"] = df["Patient_ID"].astype(int)
    df["Age"] = df["Age"].astype(int)
    df["Gender"] = df["Gender"].astype("string")
    df["Symptoms"] = df["Symptoms"].astype("string")
    df["Symptom_Count"] = df["Symptom_Count"].astype(int)
    df["Disease"] = df["Disease"].astype("string")

    return df


def save_as_parquet(df: pd.DataFrame, output_dir: Path | None = None) -> Path:
    """
    Save the DataFrame as Parquet inside data/processed and return the path.
    """
    out_dir = output_dir or paths.processed_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "healthcare_raw.parquet"
    df.to_parquet(out_path, index=False)
    return out_path


def run_ingestion() -> Path:
    """
    Simple ingestion pipeline:
      1. Read raw CSV
      2. Cast basic types
      3. Save as Parquet
    """
    df = load_raw_data()
    out_path = save_as_parquet(df)
    print(f"[INGEST] Saved processed raw data to {out_path}")
    return out_path


if __name__ == "__main__":
    run_ingestion()
