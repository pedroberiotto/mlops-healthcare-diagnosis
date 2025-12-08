from pathlib import Path

import pandas as pd
import pandera as pa

from healthcare_mlops.config import paths
from healthcare_mlops.features.label_groups import (
    add_disease_group_column,
    VALID_GROUP_LABELS,
)


# --------- Schema definition using DataFrameSchema --------- #

healthcare_schema = pa.DataFrameSchema(
    {
        "Patient_ID": pa.Column(
            int,
            checks=pa.Check.ge(1),
            nullable=False,
            coerce=True,
        ),
        "Age": pa.Column(
            int,
            checks=[pa.Check.ge(0), pa.Check.le(120)],
            nullable=False,
            coerce=True,
        ),
        "Gender": pa.Column(
            str,
            checks=pa.Check.isin(["Male", "Female", "Other"]),
            nullable=False,
            coerce=True,
        ),
        "Symptoms": pa.Column(
            str,
            checks=pa.Check.str_length(min_value=1),
            nullable=False,
            coerce=True,
        ),
        "Symptom_Count": pa.Column(
            int,
            checks=[pa.Check.ge(1), pa.Check.le(10)],
            nullable=False,
            coerce=True,
        ),
        # Original fine-grained label (kept for inspection, not used in training)
        "Disease": pa.Column(
            str,
            checks=pa.Check.str_length(min_value=1),
            nullable=False,
            coerce=True,
        ),
        # New grouped label – this is what we model
        "Disease_Group": pa.Column(
            str,
            checks=pa.Check.isin(VALID_GROUP_LABELS),
            nullable=False,
            coerce=True,
        ),
    },
    strict=True,  # do not allow extra columns
    coerce=True,
)


def validate_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the DataFrame against the healthcare schema.

    Raises a SchemaError if validation fails.
    """
    return healthcare_schema.validate(df)


def run_validation(parquet_path: Path | None = None) -> pd.DataFrame:
    """
    Read the parquet produced by ingestion, add Disease_Group,
    validate it, and save a validated parquet file.
    """
    p_path = parquet_path or (paths.processed_dir / "healthcare_raw.parquet")
    if not p_path.exists():
        raise FileNotFoundError(f"Processed raw parquet not found at {p_path}")

    df = pd.read_parquet(p_path)

    # Add the grouped label column
    df_with_group = add_disease_group_column(df, drop_original=False)

    validated_df = validate_dataframe(df_with_group)

    out_path = paths.processed_dir / "healthcare_validated.parquet"
    validated_df.to_parquet(out_path, index=False)
    print(f"[VALIDATE] Saved validated data with Disease_Group to {out_path}")
    return validated_df


if __name__ == "__main__":
    run_validation()
