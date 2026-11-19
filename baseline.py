#!/usr/bin/env python3
"""Baseline pengujian (Tema 13).

- mode `sequential` : satu proses, semua target (T1) -> dasar speedup
- mode `local`      : multiprocessing pada satu komputer (uji poin b)
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402


def probe_batch(payload):
    targets, probe = payload
    return [common.probe_target(ip, probe) for ip in targets]


def run_sequential(targets, probe):
    return [common.probe_target(ip, probe) for ip in targets]


def run_local(targets, probe, cores):
    parts = common.split_evenly(targets, cores)
    payloads = [(part, probe) for part in parts]
    rows = []
    with ProcessPoolExecutor(max_workers=cores) as pool:
        for part_rows in pool.map(probe_batch, payloads, chunksize=1):
            rows.extend(part_rows)
    return rows


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Baseline monitoring jaringan")
    parser.add_argument("--targets", default="targets.txt")
    parser.add_argument("--synth", type=int, default=0)
    parser.add_argument("--seed", type=int, default=70)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--probe-kind", choices=["tcp", "icmp"], default="tcp")
    parser.add_argument("--probe-port", type=int, default=80)
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=0.8)
    parser.add_argument("--mode", choices=["sequential", "local"], default="sequential")
    parser.add_argument("--cores", type=int, default=0)
    parser.add_argument("--out", default="hasil")
    parser.add_argument("--run-tag", default="")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.synth:
        targets = common.synth_targets(args.synth, seed=args.seed)
    else:
        targets = common.load_targets(args.targets, repeat=args.repeat, limit=args.limit)

    probe = {
        "kind": args.probe_kind,
        "port": args.probe_port,
        "count": args.count,
        "timeout": args.timeout,
    }
    tag = args.run_tag or ("sequential" if args.mode == "sequential" else "local")
    cores = max(1, args.cores or 1)

    print(
        f"[baseline] mode={args.mode} target={len(targets)} "
        f"probe={probe['kind']}:{probe['port']} count={probe['count']}"
    )

    started = time.perf_counter()
    if args.mode == "sequential":
        rows = run_sequential(targets, probe)
    else:
        rows = run_local(targets, probe, cores)
    duration = time.perf_counter() - started

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    rows_path = out_dir / f"rows_{tag}.csv"
    summary_path = out_dir / f"ringkasan_{tag}.json"

    common.write_rows_csv(rows_path, rows)
    summary = common.summarize_rows(rows)
    payload = {
        "run_tag": tag,
        "mode": args.mode,
        "cores": None if args.mode == "sequential" else cores,
        "probe": probe,
        "targets_total": len(targets),
        "tasks_total": len(targets),
        "tasks_done": len(targets),
        "tasks_failed": 0,
        "duration_s": round(duration, 3),
        "throughput_targets_s": round(len(targets) / duration, 2) if duration else 0.0,
        "rows": len(rows),
        "up": summary.get("up"),
        "down": summary.get("down"),
        "avg_rtt_ms": summary.get("avg_rtt"),
        "avg_loss_pct": summary.get("avg_loss_pct"),
    }
    summary_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print(
        f"[baseline] selesai: {duration:.2f} s, "
        f"{payload['throughput_targets_s']:.2f} target/detik -> {summary_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
