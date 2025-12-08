from __future__ import annotations

from typing import Dict, List

import pandas as pd

# Mapping from original 30 diseases to broader disease groups
DISEASE_TO_GROUP: Dict[str, str] = {
    # Respiratory / Allergy
    "Allergy": "Respiratory / Allergy",
    "Asthma": "Respiratory / Allergy",
    "Bronchitis": "Respiratory / Allergy",
    "Common Cold": "Respiratory / Allergy",
    "COVID-19": "Respiratory / Allergy",
    "Influenza": "Respiratory / Allergy",
    "Pneumonia": "Respiratory / Allergy",
    "Sinusitis": "Respiratory / Allergy",
    "Tuberculosis": "Respiratory / Allergy",

    # Cardio-metabolic & vascular
    "Heart Disease": "Cardio-Metabolic",
    "Hypertension": "Cardio-Metabolic",
    "Stroke": "Cardio-Metabolic",
    "Diabetes": "Cardio-Metabolic",
    "Obesity": "Cardio-Metabolic",

    # Gastrointestinal & liver
    "Food Poisoning": "Gastrointestinal / Liver",
    "Gastritis": "Gastrointestinal / Liver",
    "IBS": "Gastrointestinal / Liver",
    "Liver Disease": "Gastrointestinal / Liver",
    "Ulcer": "Gastrointestinal / Liver",

    # Neurological
    "Epilepsy": "Neurological",
    "Migraine": "Neurological",
    "Parkinson's": "Neurological",
    "Dementia": "Neurological",

    # Mental health
    "Anxiety": "Mental Health",
    "Depression": "Mental Health",

    # Musculoskeletal & skin
    "Arthritis": "Musculoskeletal / Skin",
    "Dermatitis": "Musculoskeletal / Skin",

    # Chronic organ / endocrine / blood
    "Chronic Kidney Disease": "Chronic Organ / Endocrine / Blood",
    "Anemia": "Chronic Organ / Endocrine / Blood",
    "Thyroid Disorder": "Chronic Organ / Endocrine / Blood",
}

VALID_GROUP_LABELS: List[str] = sorted(set(DISEASE_TO_GROUP.values()))


def add_disease_group_column(df: pd.DataFrame, *, drop_original: bool = False) -> pd.DataFrame:
    """Add a `Disease_Group` column based on `Disease`.

    If `drop_original=True`, the original `Disease` column is removed.
    Raises if any disease in the dataframe has no mapping.
    """
    df = df.copy()
    if "Disease" not in df.columns:
        raise KeyError("Expected 'Disease' column in dataframe to build Disease_Group.")

    df["Disease_Group"] = df["Disease"].map(DISEASE_TO_GROUP)

    if df["Disease_Group"].isna().any():
        unknown = df.loc[df["Disease_Group"].isna(), "Disease"].unique()
        raise ValueError(f"Found diseases without group mapping: {unknown}")

    if drop_original:
        df = df.drop(columns=["Disease"])

    return df
