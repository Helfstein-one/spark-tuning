"""Artifact sanitization and sensitive credential redaction."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable


@dataclass(frozen=True)
class RedactionReport:
    """Summary of secrets and PII redacted during artifact sanitization."""

    total_redactions: int = 0
    redacted_paths: tuple[str, ...] = ()
    categories: dict[str, int] = field(default_factory=dict)


_SENSITIVE_KEY_TOKENS: frozenset[str] = frozenset(
    {
        "pass",
        "passwd",
        "password",
        "passphrase",
        "secret",
        "secrets",
        "token",
        "tokens",
        "credential",
        "credentials",
        "auth",
        "authorization",
        "oauth",
        "key",
        "keys",
        "apikey",
        "secretkey",
        "privatekey",
        "accesskey",
        "jwt",
        "bearer",
        "pem",
    }
)

_SENSITIVE_KEY_SUBSTRINGS: tuple[str, ...] = (
    "password",
    "passwd",
    "passphrase",
    "secret",
    "token",
    "credential",
    "apikey",
    "api_key",
    "privatekey",
    "private_key",
    "accesskey",
    "access_key",
    "bearer",
)

# Strict precedence order: multi-line PEM & structured tokens first, generic KV fallback last.
_TEXT_DETECTORS: tuple[tuple[str, re.Pattern[str], str], ...] = (
    (
        "pem_key",
        re.compile(
            r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?-----"
            r"[\s\S]*?"
            r"-----END [A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?-----"
        ),
        "[REDACTED:PEM_KEY]",
    ),
    (
        "pem_key",
        re.compile(
            r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?-----[^\n]*"
            r"(?:\n[A-Za-z0-9+/=]{16,}[^\n]*)*"
        ),
        "[REDACTED:PEM_KEY]",
    ),
    (
        "signed_url",
        re.compile(
            r"(?i)(?P<base>(?:https?|s3a?|gs|abfss?|wasbs?)://[^\s?#\"'<>]+)"
            r"\?(?P<query>(?:[^\s\"'<>]*?&)?(?:X-Amz-Signature|X-Amz-Credential|"
            r"X-Amz-Security-Token|X-Goog-Signature|X-Goog-Credential|"
            r"AWSAccessKeyId|GoogleAccessId|Signature|sig)=[^\s\"'<>]*)"
        ),
        r"\g<base>?[REDACTED:SIGNED_URL]",
    ),
    (
        "url_credentials",
        re.compile(
            r"(?i)((?:https?|s3a?|gs|abfss?|wasbs?)://)"
            r"([^:@/\s\"'<>]+:[^@/\s\"'<>]+)@"
        ),
        r"\1[REDACTED:URL_CREDENTIALS]@",
    ),
    (
        "jwt",
        re.compile(
            r"(?<![A-Za-z0-9_-])"
            r"eyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}"
            r"(?![A-Za-z0-9_-])"
        ),
        "[REDACTED:JWT]",
    ),
    (
        "bearer",
        re.compile(
            r"(?i)(Authorization\s*[:=]\s*Bearer\s+)(?!\[REDACTED\b)([^\s,;\"']+)"
        ),
        r"\1[REDACTED:BEARER]",
    ),
    (
        "bearer",
        re.compile(
            r"(?i)\b(Bearer)\s+(?!\[REDACTED\b)([A-Za-z0-9._~+/=-]{8,})"
        ),
        r"\1 [REDACTED:BEARER]",
    ),
    (
        "basic",
        re.compile(
            r"(?i)(Authorization\s*[:=]\s*Basic\s+)(?!\[REDACTED\b)([^\s,;\"']+)"
        ),
        r"\1[REDACTED:BASIC]",
    ),
    (
        "basic",
        re.compile(
            r"(?i)\b(Basic)\s+(?!\[REDACTED\b)"
            r"(?=[A-Za-z0-9+/=]*[0-9+/=])([A-Za-z0-9+/]{8,}={0,2})(?![A-Za-z0-9+/=])"
        ),
        r"\1 [REDACTED:BASIC]",
    ),
    (
        "github_token",
        re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{20,}\b"),
        "[REDACTED:GITHUB_TOKEN]",
    ),
    (
        "github_token",
        re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
        "[REDACTED:GITHUB_TOKEN]",
    ),
    (
        "aws_key",
        re.compile(r"\b(?:AKIA|ASIA|ABIA|ACCA)[0-9A-Z]{16}\b"),
        "[REDACTED:AWS_KEY]",
    ),
    (
        "gcp_key",
        re.compile(r"\bAIza[0-9A-Za-z_-]{35,}\b"),
        "[REDACTED:GCP_KEY]",
    ),
    (
        "gcp_key",
        re.compile(r"\bya29\.[0-9A-Za-z_-]{20,}\b"),
        "[REDACTED:GCP_KEY]",
    ),
    (
        "azure_key",
        re.compile(
            r"(?i)(AccountKey|SharedAccessKey|SharedAccessSignature)="
            r"(?!\[REDACTED\b)([^;\s\"']+)"
        ),
        r"\1=[REDACTED:AZURE_KEY]",
    ),
    (
        "email",
        re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
        "[REDACTED:EMAIL]",
    ),
    (
        "local_path",
        re.compile(
            r"(?<![A-Za-z0-9_:])(?:/Users|/home)/[A-Za-z0-9._-]+(?:/[^\s\"'`,;)}\]]*)*"
        ),
        "[REDACTED:LOCAL_PATH]",
    ),
    (
        "local_path",
        re.compile(
            r"(?<![A-Za-z0-9_])[A-Za-z]:\\Users\\[A-Za-z0-9._-]+(?:\\[^\s\"'`,;)}\]]*)*"
        ),
        "[REDACTED:LOCAL_PATH]",
    ),
    (
        "secret",
        re.compile(
            r"(?i)"
            r"((?:(?<![a-z0-9])password|(?<![a-z0-9])passwd|(?<![a-z0-9])passphrase|"
            r"(?<![a-z0-9])secret|(?<![a-z0-9])token|(?<![a-z0-9])credential|"
            r"private_?key|api_?key|access_?key|secret_?key|"
            r"auth(?:_?(?:token|key))?|(?<![a-z0-9-])key\b)"
            r"[\"']?[ \t]*[:=][ \t]*[\"']?)"
            r"(?!Bearer\b|Basic\b|\[REDACTED\b)"
            r"([^\s,;\"'&]+)"
        ),
        r"\1[REDACTED:SECRET]",
    ),
)


class ArtifactSanitizer:
    """Sanitizes event logs, execution configs, and parameters to prevent secret/PII leaks."""

    SECRET_PATTERNS = [pattern for _, pattern, _ in _TEXT_DETECTORS]

    SAFE_ALLOWLIST: frozenset[str] = frozenset()

    @classmethod
    def _is_sensitive_key(cls, key: str) -> bool:
        camel_split = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", key).lower()
        tokens = [tok for tok in re.split(r"[^a-z0-9]+", camel_split) if tok]
        if any(tok in _SENSITIVE_KEY_TOKENS for tok in tokens):
            return True
        lower_k = key.lower()
        return any(sub in lower_k for sub in _SENSITIVE_KEY_SUBSTRINGS)

    @classmethod
    def _sanitize_string(cls, text: str) -> tuple[str, dict[str, int]]:
        if not text:
            return text, {}
        sanitized = text
        counts: dict[str, int] = {}
        for category, pattern, replacement in _TEXT_DETECTORS:
            sanitized, n = pattern.subn(replacement, sanitized)
            if n > 0:
                counts[category] = counts.get(category, 0) + n
        return sanitized, counts

    @classmethod
    def _record(
        cls,
        categories_acc: dict[str, int],
        paths_acc: list[str],
        path: str,
        counts: dict[str, int],
    ) -> None:
        if not counts:
            return
        for cat, cnt in counts.items():
            categories_acc[cat] = categories_acc.get(cat, 0) + cnt
        paths_acc.append(path or "$")

    @classmethod
    def _sanitize_value(
        cls,
        obj: Any,
        path: str,
        allowset: frozenset[str],
        memo: dict[int, Any],
        active_immutables: set[int],
        categories_acc: dict[str, int],
        paths_acc: list[str],
        force_redact_scalar: bool = False,
    ) -> Any:
        if obj is None or isinstance(obj, bool):
            return obj

        if isinstance(obj, Enum):
            if isinstance(obj.value, str):
                sanitized, counts = cls._sanitize_string(obj.value)
                if counts:
                    cls._record(categories_acc, paths_acc, path, counts)
                    return sanitized
                if force_redact_scalar and obj.value:
                    cls._record(categories_acc, paths_acc, path, {"secret": 1})
                    return "[REDACTED:SECRET]"
            return obj

        if isinstance(obj, Path):
            raw_path = str(obj)
            sanitized, counts = cls._sanitize_string(raw_path)
            if counts:
                cls._record(categories_acc, paths_acc, path, counts)
                return sanitized
            if force_redact_scalar and raw_path:
                cls._record(categories_acc, paths_acc, path, {"secret": 1})
                return "[REDACTED:SECRET]"
            return obj

        if isinstance(obj, str):
            sanitized, counts = cls._sanitize_string(obj)
            if counts:
                cls._record(categories_acc, paths_acc, path, counts)
                return sanitized
            if (
                force_redact_scalar
                and obj
                and not obj.startswith("[REDACTED")
            ):
                cls._record(categories_acc, paths_acc, path, {"secret": 1})
                return "[REDACTED:SECRET]"
            return obj

        if isinstance(obj, (int, float)):
            if force_redact_scalar:
                cls._record(categories_acc, paths_acc, path, {"secret": 1})
                return "[REDACTED:SECRET]"
            return obj

        if isinstance(obj, dict):
            obj_id = id(obj)
            if obj_id in memo:
                return memo[obj_id]
            out_dict: dict[Any, Any] = {}
            memo[obj_id] = out_dict
            try:
                for k, v in obj.items():
                    if isinstance(k, str):
                        k_sanitized, k_counts = cls._sanitize_string(k)
                        key_path = f"{path}.{k_sanitized}" if path else k_sanitized
                        if k_counts:
                            cls._record(categories_acc, paths_acc, key_path, k_counts)
                        raw_key_str = k
                        out_key: Any = k_sanitized
                    else:
                        raw_key_str = str(k)
                        key_path = f"{path}.{raw_key_str}" if path else raw_key_str
                        out_key = k

                    is_allowlisted = (
                        raw_key_str in allowset
                        or str(out_key) in allowset
                        or key_path in allowset
                    )
                    child_force = (not is_allowlisted) and cls._is_sensitive_key(raw_key_str)

                    if isinstance(v, dict):
                        out_dict[out_key] = cls._sanitize_value(
                            v,
                            key_path,
                            allowset,
                            memo,
                            active_immutables,
                            categories_acc,
                            paths_acc,
                            force_redact_scalar=False,
                        )
                    else:
                        out_dict[out_key] = cls._sanitize_value(
                            v,
                            key_path,
                            allowset,
                            memo,
                            active_immutables,
                            categories_acc,
                            paths_acc,
                            force_redact_scalar=child_force,
                        )
            finally:
                memo.pop(obj_id, None)
            return out_dict

        if isinstance(obj, list):
            obj_id = id(obj)
            if obj_id in memo:
                return memo[obj_id]
            out_list: list[Any] = []
            memo[obj_id] = out_list
            try:
                for idx, item in enumerate(obj):
                    item_path = f"{path}[{idx}]" if path else f"[{idx}]"
                    out_list.append(
                        cls._sanitize_value(
                            item,
                            item_path,
                            allowset,
                            memo,
                            active_immutables,
                            categories_acc,
                            paths_acc,
                            force_redact_scalar=force_redact_scalar and not isinstance(item, dict),
                        )
                    )
            finally:
                memo.pop(obj_id, None)
            return out_list

        if isinstance(obj, tuple):
            obj_id = id(obj)
            if obj_id in active_immutables:
                return ("<cycle>",)
            active_immutables.add(obj_id)
            try:
                items = [
                    cls._sanitize_value(
                        item,
                        f"{path}[{idx}]" if path else f"[{idx}]",
                        allowset,
                        memo,
                        active_immutables,
                        categories_acc,
                        paths_acc,
                        force_redact_scalar=force_redact_scalar and not isinstance(item, dict),
                    )
                    for idx, item in enumerate(obj)
                ]
            finally:
                active_immutables.discard(obj_id)
            return tuple(items)

        if isinstance(obj, (set, frozenset)):
            obj_id = id(obj)
            if obj_id in active_immutables:
                return type(obj)(("<cycle>",))
            active_immutables.add(obj_id)
            try:
                items = [
                    cls._sanitize_value(
                        item,
                        f"{path}[]",
                        allowset,
                        memo,
                        active_immutables,
                        categories_acc,
                        paths_acc,
                        force_redact_scalar=force_redact_scalar and not isinstance(item, dict),
                    )
                    for item in obj
                ]
            finally:
                active_immutables.discard(obj_id)
            return type(obj)(items)

        return obj

    @classmethod
    def sanitize_text(cls, text: str) -> str:
        if not isinstance(text, str):
            text = str(text)
        sanitized, _ = cls._sanitize_string(text)
        return sanitized

    @classmethod
    def sanitize_with_report(
        cls,
        data: Any,
        allowlist: Iterable[str] | None = None,
    ) -> tuple[Any, RedactionReport]:
        allowset = (
            frozenset(str(item) for item in allowlist)
            if allowlist is not None
            else cls.SAFE_ALLOWLIST
        )
        categories_acc: dict[str, int] = {}
        paths_acc: list[str] = []
        sanitized = cls._sanitize_value(
            data,
            path="",
            allowset=allowset,
            memo={},
            active_immutables=set(),
            categories_acc=categories_acc,
            paths_acc=paths_acc,
        )
        total = sum(categories_acc.values())
        report = RedactionReport(
            total_redactions=total,
            redacted_paths=tuple(dict.fromkeys(paths_acc)),
            categories=categories_acc,
        )
        return sanitized, report

    @classmethod
    def sanitize_dict(
        cls,
        data: Dict[str, Any],
        allowlist: Iterable[str] | None = None,
    ) -> Dict[str, Any]:
        sanitized, _ = cls.sanitize_with_report(data, allowlist=allowlist)
        return sanitized
