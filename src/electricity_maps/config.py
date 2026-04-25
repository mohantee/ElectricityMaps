"""Configuration loader for the Electricity Maps ETL pipeline.

Loads environment-specific config from ``config/env_{env}.properties`` files.
Uses pydantic-settings for type-safe configuration management.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# ---------------------------------------------------------------------------
# Resolve the env file path *once* at import time based on EMAPS_ENV
# ---------------------------------------------------------------------------
_ENV = os.getenv("EMAPS_ENV", "prod")
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = str(_PROJECT_ROOT / "config" / f"env_{_ENV}.properties")


class Settings(BaseSettings):
    """Typed configuration for the Electricity Maps ETL pipeline.

    Field names match the env-var names (case-insensitive) so that
    pydantic-settings can resolve them from both the ``.properties``
    file and real environment variables.
    """

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ---- Pipeline config (EMAPS_ prefix) ----
    emaps_env: str = "prod"
    emaps_api_key: str = ""
    emaps_api_base_url: str = "https://api.electricitymaps.com/v4"
    emaps_data_dir: str = "s3://electricity-maps/data"
    emaps_zone: str = "FR"

    # ---- AWS credentials ----
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    aws_region: str = "ap-south-1"

    # ---- LLM config (optional, for RAG chatbot) ----
    llm_api_key: str = ""
    llm_model: str = "gpt-4o-mini"

    # ---- Convenience properties ----

    @property
    def api_key(self) -> str:
        return self.emaps_api_key

    @property
    def api_base_url(self) -> str:
        return self.emaps_api_base_url

    @property
    def data_dir(self) -> str:
        return self.emaps_data_dir

    @property
    def zone(self) -> str:
        return self.emaps_zone

    @property
    def storage_options(self) -> dict[str, str]:
        """S3 storage options dict for Polars / DeltaLake / deltalake-rs."""
        return {
            "AWS_ACCESS_KEY_ID": self.aws_access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.aws_secret_access_key,
            "AWS_REGION": self.aws_region,
        }

    @property
    def bronze_dir(self) -> str:
        return f"{self.data_dir}/bronze"

    @property
    def silver_dir(self) -> str:
        return f"{self.data_dir}/silver"

    @property
    def gold_dir(self) -> str:
        return f"{self.data_dir}/gold"

    @property
    def state_dir(self) -> str:
        return f"{self.data_dir}/state"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached ``Settings`` instance."""
    return Settings()
