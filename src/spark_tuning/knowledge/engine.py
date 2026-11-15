"""Cross-repository centralized knowledge engine and pattern memory."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional
from spark_tuning.models import Decision, Metrics


@dataclass
class KnowledgePattern:
    pattern_hash: str
    code_fingerprint: str
    target_skill: str
    success_rate: float
    verified_speedup_ratio: float
    antipattern_warning: Optional[str] = None
    discovered_in_repo: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class CrossRepoKnowledgeEngine:
    """Stores shared intelligence across all client repositories to eliminate blind trial-and-error."""

    def __init__(self):
        self.knowledge_base: Dict[str, KnowledgePattern] = {}

    def compute_fingerprint(self, code_snippet: str) -> str:
        # Normalized whitespace hash
        normalized = "".join(code_snippet.split())
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]

    def register_success(
        self,
        code_snippet: str,
        skill_name: str,
        speedup_ratio: float,
        source_repo: str,
    ) -> KnowledgePattern:
        fp = self.compute_fingerprint(code_snippet)
        pattern = KnowledgePattern(
            pattern_hash=fp,
            code_fingerprint=code_snippet[:80],
            target_skill=skill_name,
            success_rate=1.0,
            verified_speedup_ratio=speedup_ratio,
            discovered_in_repo=source_repo,
        )
        self.knowledge_base[fp] = pattern
        return pattern

    def register_failure(
        self,
        code_snippet: str,
        warning_message: str,
        source_repo: str,
    ) -> KnowledgePattern:
        fp = self.compute_fingerprint(code_snippet)
        pattern = KnowledgePattern(
            pattern_hash=fp,
            code_fingerprint=code_snippet[:80],
            target_skill="avoid",
            success_rate=0.0,
            verified_speedup_ratio=0.0,
            antipattern_warning=warning_message,
            discovered_in_repo=source_repo,
        )
        self.knowledge_base[fp] = pattern
        return pattern

    def query_prior_knowledge(self, code_snippet: str) -> Optional[KnowledgePattern]:
        fp = self.compute_fingerprint(code_snippet)
        return self.knowledge_base.get(fp)
