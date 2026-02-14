"""
Context Management Utilities
=============================
Helper functions for loading and saving business context configurations.
"""
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / "config"


def get_business_contexts() -> dict:
    """Load business contexts from config file."""
    config_file = CONFIG_DIR / "business_contexts.json"
    with open(config_file, 'r') as f:
        return json.load(f)


def save_business_contexts(contexts: dict) -> None:
    """Save business contexts to config file."""
    config_file = CONFIG_DIR / "business_contexts.json"
    with open(config_file, 'w') as f:
        json.dump(contexts, f, indent=2)
