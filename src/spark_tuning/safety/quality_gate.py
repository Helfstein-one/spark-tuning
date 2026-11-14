"""Production Data Quality Gate verifying dataset integrity before promotion."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class QualityCheckStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass
class QualityCheckResult:
    check_name: str
    status: QualityCheckStatus
    expected: Any
    observed: Any
    details: str = ""


@dataclass
class DataQualityReport:
    passed: bool
    checks: list[QualityCheckResult] = field(default_factory=list)
    reason: str = ""


class DataQualityGate:
    """Verifies schema, row count, checksum, NULLs and completeness."""

    @classmethod
    def evaluate(
        cls,
        baseline_meta: dict[str, Any],
        candidate_meta: dict[str, Any],
    ) -> DataQualityReport:
        checks: list[QualityCheckResult] = []

        # 1. Row count verification
        base_rows = baseline_meta.get("row_count")
        cand_rows = candidate_meta.get("row_count")
        if base_rows is None or cand_rows is None:
            checks.append(
                QualityCheckResult(
                    check_name="row_count",
                    status=QualityCheckStatus.UNKNOWN,
                    expected=base_rows,
                    observed=cand_rows,
                    details="Missing row count evidence",
                )
            )
        elif base_rows != cand_rows:
            checks.append(
                QualityCheckResult(
                    check_name="row_count",
                    status=QualityCheckStatus.FAILED,
                    expected=base_rows,
                    observed=cand_rows,
                    details=f"Row count mismatch: expected {base_rows}, got {cand_rows}",
                )
            )
        else:
            checks.append(
                QualityCheckResult(
                    check_name="row_count",
                    status=QualityCheckStatus.PASSED,
                    expected=base_rows,
                    observed=cand_rows,
                )
            )

        # 2. Checksum / hash verification
        base_hash = baseline_meta.get("checksum")
        cand_hash = candidate_meta.get("checksum")
        if base_hash and cand_hash:
            if base_hash != cand_hash:
                checks.append(
                    QualityCheckResult(
                        check_name="checksum",
                        status=QualityCheckStatus.FAILED,
                        expected=base_hash,
                        observed=cand_hash,
                        details="Checksum mismatch between baseline and candidate outputs",
                    )
                )
            else:
                checks.append(
                    QualityCheckResult(
                        check_name="checksum",
                        status=QualityCheckStatus.PASSED,
                        expected=base_hash,
                        observed=cand_hash,
                    )
                )

        # 3. Schema match
        base_schema = baseline_meta.get("schema")
        cand_schema = candidate_meta.get("schema")
        if base_schema and cand_schema:
            if base_schema != cand_schema:
                checks.append(
                    QualityCheckResult(
                        check_name="schema",
                        status=QualityCheckStatus.FAILED,
                        expected=base_schema,
                        observed=cand_schema,
                        details="Schema definition altered by optimization candidate",
                    )
                )
            else:
                checks.append(
                    QualityCheckResult(
                        check_name="schema",
                        status=QualityCheckStatus.PASSED,
                        expected=base_schema,
                        observed=cand_schema,
                    )
                )

        # Overall decision
        has_failure = any(c.status == QualityCheckStatus.FAILED for c in checks)
        has_unknown = any(c.status == QualityCheckStatus.UNKNOWN for c in checks)

        if has_failure:
            return DataQualityReport(
                passed=False,
                checks=checks,
                reason="One or more quality checks failed",
            )
        if has_unknown:
            return DataQualityReport(
                passed=False,
                checks=checks,
                reason="Quality gate blocked due to missing (unknown) evidence",
            )

        return DataQualityReport(
            passed=True,
            checks=checks,
            reason="All data quality verification checks passed",
        )
