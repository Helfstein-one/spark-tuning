"""Observability and OpenLineage adapter for experiment tracking."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from spark_tuning.models import Metrics


@dataclass
class RunEvent:
    run_id: str
    event_type: str  # START, COMPLETE, FAIL
    inputs: List[str] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)
    metrics: Optional[Dict[str, Any]] = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class LineageObserver:
    """Collects and exports standard lineage run events."""

    def __init__(self, export_dir: Path | str = "./artifacts/lineage"):
        self.export_dir = Path(export_dir)
        self.export_dir.mkdir(parents=True, exist_ok=True)

    def record_run_event(self, event: RunEvent) -> Path:
        target_file = self.export_dir / f"{event.run_id}_{event.event_type.lower()}.json"
        target_file.write_text(json.dumps(event.__dict__, indent=2, default=str))
        return target_file
