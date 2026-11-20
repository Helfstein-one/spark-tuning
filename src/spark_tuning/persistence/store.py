"""Persistence store for LoopState, checkpoints, and leases."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from spark_tuning.models import Decision, Iteration, LoopPhase, Metrics, RunSpec, RunState
from spark_tuning.safety.sanitizer import ArtifactSanitizer


@dataclass
class StoredRun:
    run_id: str
    skill: str
    objective: str
    state: RunState
    decision: Decision
    reason: str
    iterations: list[Iteration] = field(default_factory=list)
    tried_hypotheses: list[str] = field(default_factory=list)
    phases: list[LoopPhase] = field(default_factory=list)
    total_cost_usd: float = 0.0


class LoopStateStore:
    """SQLite-backed or JSON-backed idempotent persistence store with leases and crash recovery."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.is_json = self.db_path.suffix.lower() == ".json"
        self._init_storage()

    def _init_storage(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if self.is_json:
            if not self.db_path.exists():
                self.db_path.write_text(json.dumps({"runs": {}, "leases": {}}), encoding="utf-8")
        else:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS runs (
                        run_id TEXT PRIMARY KEY,
                        skill TEXT,
                        objective TEXT,
                        state TEXT,
                        decision TEXT,
                        reason TEXT,
                        total_cost_usd REAL,
                        data_json TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS iterations (
                        run_id TEXT,
                        number INTEGER,
                        hypothesis TEXT,
                        decision TEXT,
                        reason TEXT,
                        metrics_json TEXT,
                        PRIMARY KEY (run_id, number)
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS leases (
                        run_id TEXT PRIMARY KEY,
                        owner TEXT,
                        expires_at REAL
                    )
                    """
                )
                conn.commit()

    def _read_json(self) -> dict[str, Any]:
        raw = self.db_path.read_text(encoding="utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("Corrupt state JSON: root must be a dict")
        return data

    def _write_json(self, data: dict[str, Any]) -> None:
        sanitized = ArtifactSanitizer.sanitize_dict(data)
        self.db_path.write_text(json.dumps(sanitized, indent=2), encoding="utf-8")

    def acquire_lease(self, run_id: str, owner: str, ttl_seconds: float) -> bool:
        now = time.time()
        expires_at = now + ttl_seconds
        if self.is_json:
            data = self._read_json()
            leases = data.setdefault("leases", {})
            existing = leases.get(run_id)
            if existing and existing["expires_at"] > now and existing["owner"] != owner:
                return False
            leases[run_id] = {"owner": owner, "expires_at": expires_at}
            self._write_json(data)
            return True
        else:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute("SELECT owner, expires_at FROM leases WHERE run_id = ?", (run_id,))
                row = cur.fetchone()
                if row:
                    curr_owner, curr_expires = row
                    if curr_expires > now and curr_owner != owner:
                        return False
                    cur.execute(
                        "UPDATE leases SET owner = ?, expires_at = ? WHERE run_id = ?",
                        (owner, expires_at, run_id),
                    )
                else:
                    cur.execute(
                        "INSERT INTO leases (run_id, owner, expires_at) VALUES (?, ?, ?)",
                        (run_id, owner, expires_at),
                    )
                conn.commit()
                return True

    def release_lease(self, run_id: str, owner: str) -> None:
        if self.is_json:
            data = self._read_json()
            leases = data.setdefault("leases", {})
            if run_id in leases and leases[run_id].get("owner") == owner:
                del leases[run_id]
                self._write_json(data)
        else:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM leases WHERE run_id = ? AND owner = ?", (run_id, owner))
                conn.commit()

    def init_run(self, spec: RunSpec) -> None:
        sanitized_spec = ArtifactSanitizer.sanitize_dict(
            {
                "run_id": spec.run_id,
                "skill": spec.skill,
                "objective": spec.objective,
                "state": RunState.INTAKE.value,
                "decision": Decision.INCONCLUSIVE.value,
                "reason": "initialized",
                "total_cost_usd": 0.0,
                "tried_hypotheses": [],
                "phases": [LoopPhase.OBSERVE.value],
            }
        )
        if self.is_json:
            data = self._read_json()
            runs = data.setdefault("runs", {})
            if spec.run_id not in runs:
                runs[spec.run_id] = {
                    **sanitized_spec,
                    "iterations": [],
                }
                self._write_json(data)
        else:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO runs (run_id, skill, objective, state, decision, reason, total_cost_usd, data_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        spec.run_id,
                        sanitized_spec["skill"],
                        sanitized_spec["objective"],
                        sanitized_spec["state"],
                        sanitized_spec["decision"],
                        sanitized_spec["reason"],
                        0.0,
                        json.dumps(sanitized_spec),
                    ),
                )
                conn.commit()

    def save_iteration(self, run_id: str, iteration: Iteration) -> None:
        metrics_dict_data: dict[str, Any] = {}
        if iteration.metrics is not None:
            from spark_tuning.adapters import metrics_dict
            metrics_dict_data = metrics_dict(iteration.metrics)
        metrics_dict_data = ArtifactSanitizer.sanitize_dict(metrics_dict_data)
        iter_record = {
            "number": iteration.number,
            "hypothesis": iteration.hypothesis,
            "decision": iteration.decision.value if iteration.decision else None,
            "reason": iteration.reason,
            "metrics": metrics_dict_data,
        }
        if self.is_json:
            data = self._read_json()
            runs = data.setdefault("runs", {})
            run = runs.get(run_id)
            if not run:
                run = {
                    "run_id": run_id,
                    "skill": "",
                    "objective": "",
                    "state": RunState.INTAKE.value,
                    "decision": Decision.INCONCLUSIVE.value,
                    "reason": "",
                    "total_cost_usd": 0.0,
                    "tried_hypotheses": [],
                    "phases": [],
                    "iterations": [],
                }
                runs[run_id] = run
            existing = [it for it in run["iterations"] if it["number"] == iteration.number]
            if existing:
                idx = run["iterations"].index(existing[0])
                run["iterations"][idx] = iter_record
            else:
                run["iterations"].append(iter_record)
            if iteration.hypothesis not in run["tried_hypotheses"]:
                run["tried_hypotheses"].append(iteration.hypothesis)
            cost = metrics_dict_data.get("cost_usd")
            if isinstance(cost, (int, float)) and not existing:
                run["total_cost_usd"] += cost
            self._write_json(data)
        else:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO iterations (run_id, number, hypothesis, decision, reason, metrics_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        iteration.number,
                        iteration.hypothesis,
                        iteration.decision.value if iteration.decision else None,
                        iteration.reason,
                        json.dumps(metrics_dict_data, default=str),
                    ),
                )
                cur = conn.cursor()
                cur.execute("SELECT data_json FROM runs WHERE run_id = ?", (run_id,))
                row = cur.fetchone()
                run_data = json.loads(row[0]) if row and row[0] else {}
                tried = run_data.get("tried_hypotheses", [])
                if iteration.hypothesis not in tried:
                    tried.append(iteration.hypothesis)
                run_data["tried_hypotheses"] = tried
                cost = metrics_dict_data.get("cost_usd")
                if isinstance(cost, (int, float)):
                    run_data["total_cost_usd"] = run_data.get("total_cost_usd", 0.0) + cost
                conn.execute(
                    "UPDATE runs SET total_cost_usd = ?, data_json = ? WHERE run_id = ?",
                    (run_data.get("total_cost_usd", 0.0), json.dumps(run_data), run_id),
                )
                conn.commit()

    def update_run_status(
        self,
        run_id: str,
        state: RunState,
        decision: Decision,
        reason: str,
        phases: list[LoopPhase] | None = None,
    ) -> None:
        phase_vals = [p.value if isinstance(p, LoopPhase) else p for p in (phases or [])]
        if self.is_json:
            data = self._read_json()
            run = data.setdefault("runs", {}).setdefault(run_id, {})
            run["state"] = state.value
            run["decision"] = decision.value
            run["reason"] = reason
            if phase_vals:
                run["phases"] = phase_vals
            self._write_json(data)
        else:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute("SELECT data_json FROM runs WHERE run_id = ?", (run_id,))
                row = cur.fetchone()
                run_data = json.loads(row[0]) if row and row[0] else {}
                run_data["state"] = state.value
                run_data["decision"] = decision.value
                run_data["reason"] = reason
                if phase_vals:
                    run_data["phases"] = phase_vals
                conn.execute(
                    """
                    UPDATE runs
                    SET state = ?, decision = ?, reason = ?, data_json = ?
                    WHERE run_id = ?
                    """,
                    (state.value, decision.value, reason, json.dumps(run_data), run_id),
                )
                conn.commit()

    def load_run(self, run_id: str) -> StoredRun:
        if self.is_json:
            data = self._read_json()
            run = data.get("runs", {}).get(run_id)
            if not run:
                raise KeyError(f"Run {run_id!r} not found in state store")
            iterations: list[Iteration] = []
            for it_data in run.get("iterations", []):
                m = Metrics(**it_data["metrics"]) if it_data.get("metrics") else None
                dec = Decision(it_data["decision"]) if it_data.get("decision") else None
                iterations.append(
                    Iteration(
                        number=it_data["number"],
                        hypothesis=it_data["hypothesis"],
                        metrics=m,
                        decision=dec,
                        reason=it_data.get("reason", ""),
                    )
                )
            phases = [LoopPhase(p) for p in run.get("phases", []) if p in LoopPhase._value2member_map_]
            return StoredRun(
                run_id=run_id,
                skill=run.get("skill", ""),
                objective=run.get("objective", ""),
                state=RunState(run.get("state", RunState.INTAKE.value)),
                decision=Decision(run.get("decision", Decision.INCONCLUSIVE.value)),
                reason=run.get("reason", ""),
                iterations=iterations,
                tried_hypotheses=run.get("tried_hypotheses", []),
                phases=phases,
                total_cost_usd=run.get("total_cost_usd", 0.0),
            )
        else:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT skill, objective, state, decision, reason, total_cost_usd, data_json FROM runs WHERE run_id = ?",
                    (run_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise KeyError(f"Run {run_id!r} not found in state store")
                skill, obj, st, dec, reason, total_cost, data_json = row
                extra = json.loads(data_json) if data_json else {}

                cur.execute(
                    "SELECT number, hypothesis, decision, reason, metrics_json FROM iterations WHERE run_id = ? ORDER BY number",
                    (run_id,),
                )
                it_rows = cur.fetchall()
                iterations = []
                for num, hyp, d_str, r_str, m_json in it_rows:
                    m = Metrics(**json.loads(m_json)) if m_json else None
                    d = Decision(d_str) if d_str else None
                    iterations.append(
                        Iteration(
                            number=num,
                            hypothesis=hyp,
                            metrics=m,
                            decision=d,
                            reason=r_str or "",
                        )
                    )
                phases = [LoopPhase(p) for p in extra.get("phases", []) if p in LoopPhase._value2member_map_]
                return StoredRun(
                    run_id=run_id,
                    skill=skill,
                    objective=obj,
                    state=RunState(st),
                    decision=Decision(dec),
                    reason=reason or "",
                    iterations=iterations,
                    tried_hypotheses=extra.get("tried_hypotheses", [it.hypothesis for it in iterations]),
                    phases=phases,
                    total_cost_usd=total_cost or 0.0,
                )
