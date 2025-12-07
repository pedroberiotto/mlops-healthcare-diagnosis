from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class Paths:
    raw_csv: Path = PROJECT_ROOT / "data" / "raw" / "Healthcare.csv"
    processed_dir: Path = PROJECT_ROOT / "data" / "processed"
    monitoring_dir: Path = PROJECT_ROOT / "data" / "monitoring"
    artifacts_dir: Path = PROJECT_ROOT / "artifacts"


paths = Paths()
