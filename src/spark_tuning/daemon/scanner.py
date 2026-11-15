"""Multi-tenant sandboxed repository scanner for autonomous tuning agents."""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class TargetRepository:
    tenant_id: str
    repo_url: str
    default_branch: str = "main"
    auth_token: Optional[str] = None


@dataclass
class ScannedSparkArtifacts:
    repo_name: str
    pyspark_files: List[str] = field(default_factory=list)
    sql_files: List[str] = field(default_factory=list)
    spark_conf_files: List[str] = field(default_factory=list)


class MultiTenantRepoScanner:
    """Manages ephemeral sandboxed environments for multi-tenant code scanning."""

    def __init__(self, base_workspace_dir: Optional[str | Path] = None):
        self.base_workspace_dir = Path(base_workspace_dir or tempfile.gettempdir()) / "spark_tuning_tenants"
        self.base_workspace_dir.mkdir(parents=True, exist_ok=True)

    def scan_local_target(self, repo: TargetRepository, local_dir: Path | str) -> ScannedSparkArtifacts:
        target_path = Path(local_dir)
        pyspark_files = []
        sql_files = []
        conf_files = []

        for p in target_path.rglob("*"):
            if not p.is_file() or ".git" in p.parts:
                continue
            ext = p.suffix.lower()
            if ext == ".py":
                # Quick inspection for Spark imports or usage
                try:
                    content = p.read_text(encoding="utf-8", errors="ignore")
                    if any(term in content for term in ["pyspark", "SparkSession", "DataFrame", "spark.read"]):
                        pyspark_files.append(str(p.relative_to(target_path)))
                except Exception:
                    pass
            elif ext == ".sql":
                sql_files.append(str(p.relative_to(target_path)))
            elif "spark" in p.name.lower() and ext in [".conf", ".properties", ".yaml", ".yml"]:
                conf_files.append(str(p.relative_to(target_path)))

        return ScannedSparkArtifacts(
            repo_name=repo.repo_url.split("/")[-1].replace(".git", ""),
            pyspark_files=pyspark_files,
            sql_files=sql_files,
            spark_conf_files=conf_files,
        )

    def cleanup_tenant(self, tenant_id: str) -> None:
        tenant_dir = self.base_workspace_dir / tenant_id
        if tenant_dir.exists():
            shutil.rmtree(tenant_dir, ignore_errors=True)
