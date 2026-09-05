#!/usr/bin/env python3
"""Aggregates O0-S1 storage cost probe JSONL output into markdown tables.

Usage:
    python3 o0s1_aggregate.py RUN1.jsonl [RUN2.jsonl ...] > o0s1_summary.md

Reads the JSON lines emitted by `o0s1_storage_cost_probe` (one or more whole
runs), groups `sample` lines by scenario, and reports P50 / P95 / min / max
for every numeric metric across all runs, plus the inter-run spread of the
per-run medians as measurement noise. Witness lines, concurrency summaries,
and info lines are reproduced verbatim so the evidence note can cite them.
"""

import json
import math
import sys
from collections import defaultdict


def percentile(values, fraction):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(
        len(ordered) - 1,
        max(0, math.ceil(len(ordered) * fraction) - 1),
    )
    return ordered[index]


def fmt(key, value):
    if key.endswith("_ns") and value >= 1000:
        return f"{value:,.0f} ({value / 1000:,.1f} us)"
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def main(paths):
    # (scenario, key) -> {"runs": {run_index: [values]}, "all": [...]}
    metrics = defaultdict(lambda: {"runs": defaultdict(list), "all": []})
    keys_per_scenario = defaultdict(set)
    witnesses = []
    infos = []
    conc_rows = []

    for run_index, path in enumerate(paths):
        with open(path) as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                kind = record.get("kind")
                if kind == "sample":
                    scenario = record["scenario"]
                    for key, value in record.items():
                        if key in ("kind", "scenario", "iter"):
                            continue
                        if isinstance(value, bool) or not isinstance(value, (int, float)):
                            continue
                        metrics[(scenario, key)]["runs"][run_index].append(value)
                        metrics[(scenario, key)]["all"].append(value)
                        keys_per_scenario[scenario].add(key)
                elif kind == "conc_sample":
                    scenario = record["scenario"]
                    key = f'{record["role"]}_op_ns'
                    value = record["op_ns"]
                    metrics[(scenario, key)]["runs"][run_index].append(value)
                    metrics[(scenario, key)]["all"].append(value)
                    keys_per_scenario[scenario].add(key)
                elif kind == "witness":
                    witnesses.append(record)
                elif kind == "info":
                    if "machine" in record or "calibration" in record:
                        infos.append(record)
                    elif "reader_threads" in record:
                        conc_rows.append(record)

    print("# O0-S1 storage cost probe — aggregated over "
          f"{len(paths)} run(s)\n")
    for record in infos:
        print(f"- info: {json.dumps(record, sort_keys=True)}")
    print()

    for scenario in sorted(keys_per_scenario):
        print(f"## {scenario}\n")
        print(
            "| metric | n | P50 | P95 | min | max | inter-run median spread |"
        )
        print("| --- | --- | --- | --- | --- | --- | --- |")
        for key in sorted(keys_per_scenario[scenario]):
            entry = metrics[(scenario, key)]
            all_values = entry["all"]
            run_medians = [
                percentile(values, 0.50)
                for values in entry["runs"].values()
                if values
            ]
            median_of_medians = statistics.median(run_medians) if run_medians else 0
            if median_of_medians and len(run_medians) > 1:
                spread = (max(run_medians) - min(run_medians)) / median_of_medians
                noise = f"{spread * 100:,.1f}%"
            else:
                noise = "n/a"
            print(
                f"| {key} | {len(all_values)} "
                f"| {fmt(key, percentile(all_values, 0.50))} "
                f"| {fmt(key, percentile(all_values, 0.95))} "
                f"| {fmt(key, min(all_values))} "
                f"| {fmt(key, max(all_values))} | {noise} |"
            )
        print()

    if conc_rows:
        print("## concurrency scenario summaries (one per run)\n")
        for record in conc_rows:
            print(f"- {json.dumps(record, sort_keys=True)}")
        print()

    if witnesses:
        print("## witnesses (one block per run)\n")
        for record in witnesses:
            print(f"- {json.dumps(record, sort_keys=True)}")
        print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    main(sys.argv[1:])
