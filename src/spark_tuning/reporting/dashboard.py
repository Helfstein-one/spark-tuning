"""Interactive standalone single-file HTML dashboard generator with Chart.js."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List
from spark_tuning.models import LoopResult, Metrics


class InteractiveDashboardGenerator:
    """Generates a self-contained, standalone HTML dashboard with interactive charts."""

    @classmethod
    def generate_html(
        cls,
        result: LoopResult,
        baseline: Metrics,
        output_path: Path | str | None = None,
    ) -> str:
        # Prepare data for Chart.js (keep missing telemetry as null, never coerce None -> 0)
        iterations_labels = ["Baseline (0)"]
        durations: list[float | None] = [baseline.duration_seconds]
        costs: list[float | None] = [baseline.cost_usd]
        spills: list[float | None] = [
            round(baseline.spill_disk_bytes / (1024 * 1024), 2)
            if baseline.spill_disk_bytes is not None
            else None
        ]
        gcs: list[float | None] = [baseline.gc_seconds]

        for it in result.iterations:
            iterations_labels.append(f"It {it.number}: {it.hypothesis}")
            if it.metrics:
                durations.append(it.metrics.duration_seconds)
                costs.append(it.metrics.cost_usd)
                spills.append(
                    round(it.metrics.spill_disk_bytes / (1024 * 1024), 2)
                    if it.metrics.spill_disk_bytes is not None
                    else None
                )
                gcs.append(it.metrics.gc_seconds)
            else:
                durations.append(None)
                costs.append(None)
                spills.append(None)
                gcs.append(None)

        # Build self-contained HTML
        html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Spark Tuning - Interactive Optimization Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root {{
      --bg: #090d16;
      --card-bg: #111827;
      --border: #1f2937;
      --accent-orange: #f97316;
      --accent-cyan: #06b6d4;
      --accent-green: #10b981;
      --text: #f9fafb;
      --text-muted: #9ca3af;
    }}
    body {{
      margin: 0;
      padding: 24px;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    }}
    .header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-bottom: 1px solid var(--border);
      padding-bottom: 16px;
      margin-bottom: 24px;
    }}
    .badge {{
      background: rgba(16, 185, 129, 0.2);
      color: var(--accent-green);
      border: 1px solid var(--accent-green);
      padding: 6px 14px;
      border-radius: 9999px;
      font-weight: 700;
      font-size: 13px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
      gap: 20px;
      margin-bottom: 24px;
    }}
    .card {{
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 20px;
    }}
    .card h3 {{
      margin-top: 0;
      font-size: 15px;
      color: var(--accent-cyan);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 12px;
      font-size: 13px;
    }}
    th, td {{
      padding: 10px;
      text-align: left;
      border-bottom: 1px solid var(--border);
    }}
    th {{ color: var(--text-muted); }}
  </style>
</head>
<body>
  <div class="header">
    <div>
      <h1 style="margin: 0; font-size: 24px;">Spark Tuning Dashboard</h1>
      <p style="margin: 4px 0 0; color: var(--text-muted); font-size: 13px;">Run ID: {result.run_id}</p>
    </div>
    <div class="badge">{result.decision.value.upper()}</div>
  </div>

  <div class="grid">
    <div class="card">
      <h3>Duração vs Custo por Iteração</h3>
      <canvas id="durationChart"></canvas>
    </div>
    <div class="card">
      <h3>Eliminação de Spill em Disco & GC</h3>
      <canvas id="spillChart"></canvas>
    </div>
  </div>

  <div class="card">
    <h3>Tabela de Iterações do Loop Cognitivo</h3>
    <table>
      <thead>
        <tr>
          <th>Iteração</th>
          <th>Hipótese</th>
          <th>Duração (s)</th>
          <th>Custo ($)</th>
          <th>Spill (MB)</th>
          <th>GC (s)</th>
          <th>Decisão</th>
          <th>Motivo</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>0 (Baseline)</td>
          <td>Baseline Inicial</td>
          <td>{f"{baseline.duration_seconds}s" if baseline.duration_seconds is not None else "unknown"}</td>
          <td>{f"${baseline.cost_usd}" if baseline.cost_usd is not None else "unknown"}</td>
          <td>{f"{round(baseline.spill_disk_bytes/(1024*1024), 2)} MB" if baseline.spill_disk_bytes is not None else "unknown"}</td>
          <td>{f"{baseline.gc_seconds}s" if baseline.gc_seconds is not None else "unknown"}</td>
          <td>BASELINE</td>
          <td>Referência</td>
        </tr>
"""
        for it in result.iterations:
            m = it.metrics
            dur = (f"{m.duration_seconds}s" if m.duration_seconds is not None else "unknown") if m else "-"
            c = (f"${m.cost_usd}" if m.cost_usd is not None else "unknown") if m else "-"
            sp = (
                f"{round(m.spill_disk_bytes/(1024*1024), 2)} MB"
                if m.spill_disk_bytes is not None
                else "unknown"
            ) if m else "-"
            gc = (f"{m.gc_seconds}s" if m.gc_seconds is not None else "unknown") if m else "-"
            dec = it.decision.value if it.decision else "-"
            html += f"""
        <tr>
          <td>{it.number}</td>
          <td>{it.hypothesis}</td>
          <td>{dur}</td>
          <td>{c}</td>
          <td>{sp}</td>
          <td>{gc}</td>
          <td><strong>{dec}</strong></td>
          <td>{it.reason}</td>
        </tr>
"""

        html += f"""
      </tbody>
    </table>
  </div>

  <script>
    const labels = {json.dumps(iterations_labels)};
    
    new Chart(document.getElementById('durationChart'), {{
      type: 'bar',
      data: {{
        labels: labels,
        datasets: [
          {{
            label: 'Duração (s)',
            data: {json.dumps(durations)},
            backgroundColor: '#06b6d4',
            yAxisID: 'y'
          }},
          {{
            label: 'Custo ($)',
            data: {json.dumps(costs)},
            backgroundColor: '#f97316',
            yAxisID: 'y1'
          }}
        ]
      }},
      options: {{
        responsive: true,
        scales: {{
          y: {{ type: 'linear', position: 'left', grid: {{ color: '#1f2937' }} }},
          y1: {{ type: 'linear', position: 'right', grid: {{ drawOnChartArea: false }} }}
        }}
      }}
    }});

    new Chart(document.getElementById('spillChart'), {{
      type: 'line',
      data: {{
        labels: labels,
        datasets: [
          {{
            label: 'Spill em Disco (MB)',
            data: {json.dumps(spills)},
            borderColor: '#ef4444',
            backgroundColor: 'rgba(239, 68, 68, 0.1)',
            fill: true
          }},
          {{
            label: 'Tempo GC (s)',
            data: {json.dumps(gcs)},
            borderColor: '#eab308',
            backgroundColor: 'rgba(234, 179, 8, 0.1)',
            fill: true
          }}
        ]
      }},
      options: {{
        responsive: true,
        scales: {{ grid: {{ color: '#1f2937' }} }}
      }}
    }});
  </script>
</body>
</html>
"""
        if output_path:
            p = Path(output_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(html, encoding="utf-8")

        return html
