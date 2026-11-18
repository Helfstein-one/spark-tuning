from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class RunState(str, Enum):
    INTAKE = "intake"
    BASELINE = "baseline"
    DIAGNOSE = "diagnose"
    EXPERIMENT = "experiment"
    VERIFY = "verify"
    ITERATE = "iterate"
    ACCEPTED = "accepted"
    ROLLBACK = "rollback"
    BLOCKED = "blocked"
    PR_READY = "pr_ready"


class Decision(str, Enum):
    ACCEPT = "accept"
    REJECT = "reject"
    ROLLBACK = "rollback"
    INCONCLUSIVE = "inconclusive"
    BLOCKED = "blocked"


class MetricStatus(str, Enum):
    OK = "ok"
    UNKNOWN = "unknown"
    INVALID = "invalid"


CANONICAL_UNITS: dict[str, str] = {
    "duration_seconds": "seconds",
    "cost_usd": "usd",
    "shuffle_write_bytes": "bytes",
    "spill_disk_bytes": "bytes",
    "gc_seconds": "seconds",
    "task_p95_seconds": "seconds",
    "failures": "count",
    "stages_count": "count",
    "tasks_count": "count",
    "task_retries": "count",
    "bytes_read": "bytes",
    "bytes_written": "bytes",
    "shuffle_read_bytes": "bytes",
    "spill_memory_bytes": "bytes",
}


@dataclass(frozen=True)
class MetricValue:
    value: float | int | None = None
    unit: str = ""
    source: str = "unknown"
    timestamp: str = ""
    quality: str = "unknown"
    status: MetricStatus | None = None

    def __post_init__(self) -> None:
        if isinstance(self.value, bool):
            raise ValueError("MetricValue.value cannot be a bool")
        if self.value is not None and not isinstance(self.value, (int, float)):
            raise ValueError(f"MetricValue.value must be numeric or None, got {type(self.value).__name__}")

        if self.status is None:
            if self.value is None:
                resolved = MetricStatus.UNKNOWN
            elif not math.isfinite(float(self.value)):
                resolved = MetricStatus.INVALID
            else:
                resolved = MetricStatus.OK
            object.__setattr__(self, "status", resolved)
        else:
            resolved = self.status if isinstance(self.status, MetricStatus) else MetricStatus(self.status)
            object.__setattr__(self, "status", resolved)
            if resolved is MetricStatus.OK:
                if self.value is None or not math.isfinite(float(self.value)):
                    raise ValueError("MetricValue with status OK must have a finite numeric value")

    @property
    def is_known(self) -> bool:
        return self.status is MetricStatus.OK and self.value is not None

    @property
    def is_usable(self) -> bool:
        return self.is_known and math.isfinite(float(self.value))  # type: ignore[arg-type]

    def is_comparable_to(self, other: MetricValue) -> bool:
        if not self.is_usable or not other.is_usable:
            return False
        return self.unit == other.unit

    @classmethod
    def unknown(
        cls,
        *,
        unit: str = "",
        source: str = "unknown",
        timestamp: str = "",
        quality: str = "missing",
    ) -> MetricValue:
        return cls(
            value=None,
            unit=unit,
            source=source,
            timestamp=timestamp,
            quality=quality,
            status=MetricStatus.UNKNOWN,
        )

    @classmethod
    def invalid(
        cls,
        *,
        value: float | int | None = None,
        unit: str = "",
        source: str = "unknown",
        timestamp: str = "",
        quality: str = "invalid",
    ) -> MetricValue:
        return cls(
            value=value,
            unit=unit,
            source=source,
            timestamp=timestamp,
            quality=quality,
            status=MetricStatus.INVALID,
        )


@dataclass(frozen=True)
class ParseDiagnostics:
    spark_version: str | None = None
    spark_major: int | None = None
    total_lines: int = 0
    valid_events: int = 0
    invalid_json_lines: int = 0
    unknown_events: int = 0
    truncated: bool = False
    missing_app_start: bool = False
    missing_app_end: bool = False
    completeness_score: float = 1.0
    is_complete: bool = True
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.warnings, tuple):
            object.__setattr__(self, "warnings", tuple(str(w) for w in self.warnings))
        for count_name in ("total_lines", "valid_events", "invalid_json_lines", "unknown_events"):
            val = getattr(self, count_name)
            if isinstance(val, bool) or not isinstance(val, int) or val < 0:
                raise ValueError(f"ParseDiagnostics.{count_name} must be a non-negative int, got {val!r}")
        if self.spark_major is not None:
            if isinstance(self.spark_major, bool) or not isinstance(self.spark_major, int) or self.spark_major < 0:
                raise ValueError(f"ParseDiagnostics.spark_major must be a non-negative int or None, got {self.spark_major!r}")
        if isinstance(self.completeness_score, bool) or not isinstance(self.completeness_score, (int, float)):
            raise ValueError(f"ParseDiagnostics.completeness_score must be numeric, got {self.completeness_score!r}")
        score = float(self.completeness_score)
        if not math.isfinite(score) or score < 0.0 or score > 1.0:
            raise ValueError(f"ParseDiagnostics.completeness_score must be in [0.0, 1.0], got {score!r}")


_NUMERIC_METRIC_FIELDS: tuple[str, ...] = (
    "duration_seconds",
    "cost_usd",
    "shuffle_write_bytes",
    "spill_disk_bytes",
    "gc_seconds",
    "task_p95_seconds",
    "failures",
    "stages_count",
    "tasks_count",
    "task_retries",
    "bytes_read",
    "bytes_written",
    "shuffle_read_bytes",
    "spill_memory_bytes",
)


@dataclass(frozen=True)
class Metrics:
    duration_seconds: float | None = None
    cost_usd: float | None = None
    shuffle_write_bytes: int | None = None
    spill_disk_bytes: int | None = None
    gc_seconds: float | None = None
    task_p95_seconds: float | None = None
    failures: int | None = 0
    correctness: bool | None = True
    stages_count: int | None = None
    tasks_count: int | None = None
    task_retries: int | None = None
    bytes_read: int | None = None
    bytes_written: int | None = None
    shuffle_read_bytes: int | None = None
    spill_memory_bytes: int | None = None
    source: str = "unknown"
    timestamp: str = ""
    quality: str = "measured"
    workload_id: str | None = None
    units: dict[str, str] = field(default_factory=dict)
    metric_values: dict[str, MetricValue] = field(default_factory=dict)
    parse_diagnostics: ParseDiagnostics | None = None

    def __post_init__(self) -> None:
        for field_name in _NUMERIC_METRIC_FIELDS:
            val = getattr(self, field_name)
            if val is None:
                continue
            if isinstance(val, bool) or not isinstance(val, (int, float)):
                raise ValueError(f"Metrics.{field_name} must be numeric or None, got {type(val).__name__}")
            fval = float(val)
            if not math.isfinite(fval):
                raise ValueError(f"Metrics.{field_name} must be finite, got {val!r}")
            if fval < 0.0:
                raise ValueError(f"Metrics.{field_name} must be non-negative, got {val!r}")

        if self.correctness is not None and not isinstance(self.correctness, bool):
            raise ValueError(f"Metrics.correctness must be bool or None, got {type(self.correctness).__name__}")

        if not isinstance(self.units, dict):
            object.__setattr__(self, "units", dict(self.units))

        if self.metric_values:
            coerced_mv: dict[str, MetricValue] = {}
            for key, item in self.metric_values.items():
                if isinstance(item, MetricValue):
                    coerced_mv[key] = item
                elif isinstance(item, Mapping):
                    coerced_mv[key] = MetricValue(**dict(item))
                else:
                    raise ValueError(f"Metrics.metric_values[{key!r}] must be MetricValue or mapping")
            object.__setattr__(self, "metric_values", coerced_mv)

        if isinstance(self.parse_diagnostics, Mapping):
            object.__setattr__(self, "parse_diagnostics", ParseDiagnostics(**dict(self.parse_diagnostics)))

    @property
    def correctness_ok(self) -> bool | None:
        return self.correctness

    def get_metric(self, name: str) -> MetricValue:
        if name in self.metric_values:
            return self.metric_values[name]

        unit = self.units.get(name, CANONICAL_UNITS.get(name, ""))
        if name == "correctness":
            if self.correctness is None:
                return MetricValue.unknown(
                    unit="bool",
                    source=self.source,
                    timestamp=self.timestamp,
                    quality="missing",
                )
            return MetricValue(
                value=1 if self.correctness else 0,
                unit="bool",
                source=self.source,
                timestamp=self.timestamp,
                quality=self.quality,
                status=MetricStatus.OK,
            )

        if hasattr(self, name):
            val = getattr(self, name)
            if val is None:
                return MetricValue.unknown(
                    unit=unit,
                    source=self.source,
                    timestamp=self.timestamp,
                    quality="missing",
                )
            return MetricValue(
                value=val,
                unit=unit,
                source=self.source,
                timestamp=self.timestamp,
                quality=self.quality,
                status=MetricStatus.OK,
            )

        return MetricValue.unknown(
            unit=unit,
            source=self.source,
            timestamp=self.timestamp,
            quality="missing",
        )

    def validate_comparable(self, other: Metrics) -> list[str]:
        errors: list[str] = []
        if self.workload_id is not None and other.workload_id is not None and self.workload_id != other.workload_id:
            errors.append(f"workload_id mismatch: {self.workload_id!r} != {other.workload_id!r}")

        fields_to_check = (
            "duration_seconds",
            "cost_usd",
            "task_p95_seconds",
            "spill_disk_bytes",
            "failures",
            "gc_seconds",
        )
        checked = set(fields_to_check)
        extra_keys = (set(self.units) | set(other.units) | set(self.metric_values) | set(other.metric_values)) - checked

        for field_name in (*fields_to_check, *sorted(extra_keys)):
            bm = self.get_metric(field_name)
            cm = other.get_metric(field_name)
            if bm.status is MetricStatus.INVALID:
                errors.append(f"baseline metric '{field_name}' has invalid status")
            if cm.status is MetricStatus.INVALID:
                errors.append(f"candidate metric '{field_name}' has invalid status")
            if bm.unit and cm.unit and bm.unit != cm.unit:
                errors.append(f"unit mismatch for '{field_name}': {bm.unit!r} != {cm.unit!r}")

        return errors


@dataclass(frozen=True)
class RunSpec:
    run_id: str
    skill: str
    objective: str
    baseline: Metrics
    max_iterations: int = 7
    max_total_cost_usd: float = 200.0
    max_runtime_minutes: float = 120.0
    max_changes_per_iteration: int = 1
    min_duration_improvement: float = 0.10
    min_cost_improvement: float = 0.10
    max_p95_regression: float = 0.20
    max_spill_regression: float = 0.20
    hypotheses: tuple[dict[str, Any], ...] = ()


@dataclass
class Iteration:
    number: int
    hypothesis: str
    metrics: Metrics | None = None
    decision: Decision | None = None
    reason: str = ""


@dataclass
class LoopResult:
    run_id: str
    state: RunState
    decision: Decision
    iterations: list[Iteration] = field(default_factory=list)
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
