"""Artifact sanitization and sensitive credential redaction."""

from __future__ import annotations

import re
from typing import Any, Dict


class ArtifactSanitizer:
    """Sanitizes event logs, execution configs, and parameters to prevent secret/PII leaks."""

    SECRET_PATTERNS = [
        re.compile(r"(?i)(password|secret|token|key|credential|auth)[\"':=\s]+([^\s,;\"']+)"),
        re.compile(r"gho_[A-Za-z0-9_]{30,}"),
        re.compile(r"AKIA[0-9A-Z]{16}"),
    ]

    @classmethod
    def sanitize_text(cls, text: str) -> str:
        sanitized = re.sub(
            r"(?i)(password|secret|token|key|credential|auth)[\"':=\s]+([^\s,;\"']+)",
            r"\1: [REDACTED]",
            text,
        )
        sanitized = re.sub(r"gho_[A-Za-z0-9_]{20,}", "[REDACTED_GH_TOKEN]", sanitized)
        sanitized = re.sub(r"AKIA[0-9A-Z]{16}", "[REDACTED_AWS_KEY]", sanitized)
        return sanitized

    @classmethod
    def sanitize_dict(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        result = {}
        for k, v in data.items():
            if any(term in k.lower() for term in ["pass", "token", "secret", "key"]):
                result[k] = "[REDACTED]"
            elif isinstance(v, str):
                result[k] = cls.sanitize_text(v)
            elif isinstance(v, dict):
                result[k] = cls.sanitize_dict(v)
            else:
                result[k] = v
        return result
