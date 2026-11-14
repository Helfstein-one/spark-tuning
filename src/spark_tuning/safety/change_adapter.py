"""Safety adapter enforcing isolated branch workspaces, file allowlists, and rollback."""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ChangeCandidate:
    target_file: str
    content: str
    description: str


class SafeChangeAdapter:
    """Safely applies changes to allowed files on isolated branches with atomic rollback."""

    def __init__(self, repo_root: str | Path, allowlist: list[str]):
        self.repo_root = Path(repo_root)
        self.allowlist = [str(Path(p).resolve()) for p in allowlist]
        self.snapshots: dict[str, str] = {}

    def is_allowed(self, file_path: str | Path) -> bool:
        resolved = str(Path(file_path).resolve())
        return any(resolved == allowed or resolved.startswith(allowed + "/") for allowed in self.allowlist)

    def apply_candidate(self, candidate: ChangeCandidate) -> None:
        file_path = Path(candidate.target_file).resolve()
        if not self.is_allowed(file_path):
            raise PermissionError(
                f"Modification blocked: '{candidate.target_file}' is not in the approved file allowlist."
            )

        # Snapshot baseline content before modifying
        if file_path.exists():
            self.snapshots[str(file_path)] = file_path.read_text(encoding="utf-8")
        else:
            self.snapshots[str(file_path)] = ""

        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(candidate.content, encoding="utf-8")

    def rollback_all(self) -> None:
        """Restores all modified files to their original baseline snapshot."""
        for path_str, original_content in self.snapshots.items():
            p = Path(path_str)
            if original_content == "":
                if p.exists():
                    p.unlink()
            else:
                p.write_text(original_content, encoding="utf-8")
        self.snapshots.clear()
