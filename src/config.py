"""Load configuration from YAML or environment."""
import os
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None

# Default base path: project root (where main.py lives)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_config(config_path: str | None = None) -> dict:
    """Load config from YAML file. Falls back to defaults if file missing or YAML unavailable."""
    defaults = {
        "paths": {
            "raw": "data/raw",
            "sample": "data/sample",
            "output": "output",
        },
        "input_file": "salesdaily.csv",
        "sample_file": "salesdaily_sample.csv",
        "spark": {"shuffle_partitions": 4},
        "default_mode": "sample",
    }
    if config_path and os.path.isfile(config_path):
        if yaml:
            with open(config_path) as f:
                loaded = yaml.safe_load(f) or {}
                for k, v in defaults.items():
                    if k not in loaded:
                        loaded[k] = v
                    elif isinstance(v, dict) and isinstance(loaded[k], dict):
                        for k2, v2 in v.items():
                            if k2 not in loaded[k]:
                                loaded[k][k2] = v2
                return loaded
    return defaults


def get_data_path(config: dict, mode: str) -> str:
    """Return absolute path to input file (sample or raw)."""
    base = config["paths"].get("sample" if mode == "sample" else "raw", "data/sample")
    filename = config.get("sample_file" if mode == "sample" else "input_file", "salesdaily_sample.csv" if mode == "sample" else "salesdaily.csv")
    path = _PROJECT_ROOT / base / filename
    return str(path)


def get_output_base(config: dict) -> str:
    """Return absolute path to output directory."""
    base = config["paths"].get("output", "output")
    return str(_PROJECT_ROOT / base)
