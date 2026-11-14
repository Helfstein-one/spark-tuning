"""Corpus of real-world anti-pattern Spark jobs for diagnostic proof testing."""

from __future__ import annotations

from dataclasses import dataclass
from spark_tuning.models import Metrics
from spark_tuning.diagnostics.engine import BottleneckCategory


@dataclass
class AntiPatternJobScenario:
    name: str
    category: BottleneckCategory
    description: str
    bad_code_snippet: str
    bad_metrics: Metrics
    expected_primary_cause: BottleneckCategory
    expected_recommended_skill: str


class AntiPatternCorpus:
    """Provides intentionally misconfigured or poorly coded Spark workloads."""

    @classmethod
    def get_skew_spill_scenario(cls) -> AntiPatternJobScenario:
        return AntiPatternJobScenario(
            name="cartesian_skewed_aggregation",
            category=BottleneckCategory.SPILL,
            description="Aggregating on a heavily skewed column with default 200 partitions causing severe memory spill",
            bad_code_snippet="df.groupBy('skewed_null_key').count()",
            bad_metrics=Metrics(
                duration_seconds=340.0,
                cost_usd=17.0,
                spill_disk_bytes=22 * 1024 * 1024 * 1024,  # 22 GB Spill
                task_p95_seconds=180.0,
                tasks_count=200,
                failures=0,
            ),
            expected_primary_cause=BottleneckCategory.SPILL,
            expected_recommended_skill="partition-sizing",
        )

    @classmethod
    def get_python_udf_bottleneck_scenario(cls) -> AntiPatternJobScenario:
        return AntiPatternJobScenario(
            name="cpython_row_by_row_udf",
            category=BottleneckCategory.CPU,
            description="Using standard Python @udf instead of vectorized native Spark expressions",
            bad_code_snippet="@udf('string')\ndef parse_text(s):\n    return s.lower()",
            bad_metrics=Metrics(
                duration_seconds=480.0,
                cost_usd=24.0,
                gc_seconds=15.0,
                failures=0,
            ),
            expected_primary_cause=BottleneckCategory.UNKNOWN,  # or CPU / boundaries
            expected_recommended_skill="pyspark-boundaries",
        )

    @classmethod
    def get_gc_thrashing_scenario(cls) -> AntiPatternJobScenario:
        return AntiPatternJobScenario(
            name="excessive_object_churn_gc",
            category=BottleneckCategory.MEMORY_GC,
            description="Creating billions of tiny Java wrapper objects exhausting JVM heap",
            bad_code_snippet="rdd.map(lambda x: [CustomClass(i) for i in x]).collect()",
            bad_metrics=Metrics(
                duration_seconds=290.0,
                cost_usd=14.5,
                gc_seconds=110.0,  # 38% of runtime in GC pause!
                failures=0,
            ),
            expected_primary_cause=BottleneckCategory.MEMORY_GC,
            expected_recommended_skill="jvm-gc-memory",
        )
