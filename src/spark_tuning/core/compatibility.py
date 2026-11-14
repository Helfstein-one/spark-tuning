"""Spark runtime compatibility matrix verifier."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class RuntimeProfile:
    spark_version: str
    python_version: str
    java_version: str
    cloud_platform: str  # EMR, Databricks, Dataproc, Glue, Local


class CompatibilityMatrix:
    """Verifies that candidate configs and skills are compatible with the execution environment."""

    SUPPORTED_PLATFORMS = {"local", "emr", "databricks", "dataproc", "glue"}
    MIN_SPARK_VERSION = "3.2.0"

    @classmethod
    def validate_profile(cls, profile: RuntimeProfile) -> tuple[bool, str]:
        if profile.cloud_platform.lower() not in cls.SUPPORTED_PLATFORMS:
            return False, f"Unsupported cloud platform: {profile.cloud_platform}"
        if profile.spark_version < cls.MIN_SPARK_VERSION:
            return False, f"Spark version {profile.spark_version} is below minimum supported ({cls.MIN_SPARK_VERSION})"
        return True, "Profile is fully compatible"
