#!/usr/bin/env python3
"""Uji fault tolerance otomatis (Tema 13).

Skenario:
1. Master + N worker lokal dijalankan.
2. Worker target dibunuh (SIGKILL) tepat saat sedang mengerjakan task.
3. Master harus mendeteksi, mengembalikan task ke antrean, dan worker lain
   menyelesaikannya.
4. Skrip membaca event JSONL dari master dan menulis:
   - laporan/fault_<tag>_kills.csv   : waktu kill, waktu deteksi, latency deteksi
   - laporan/fault_<tag>_tasks.csv   : recovery time per task
   - laporan/fault_<tag>_ringkas.json: ringkasan

Contoh:
    python3 faulttest.py --workers 3 --kill L1,L2 --stagger 2 --synth 200
    python3 faulttest.py --workers 2 --kill L1 --synth 60 --count 1 --timeout 0.3
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent
HASIL = BASE / "hasil"
LAPORAN = BASE / "laporan"
LOG = LAPORAN / "log"

sys.path.insert(0, str(BASE))
import benchmark  # noqa: E402  (reuse free_port/wait_port/run_proc)
import common  # noqa: E402


# ---------------------------------------------------------------------------
# Event helpers
# ---------------------------------------------------------------------------


def read_events(path: Path):
    if not path.exists():
        return []
    events = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def worker_busy(events, worker_id: str) -> bool:
    """True jika event terakhir worker tersebut adalah task_assigned."""
    current = None
    for event in events:
        if event.get("worker") != worker_id:
            continue
        kind = event.get("kind")
        if kind == "task_assigned":
            current = event.get("task")
        elif kind == "task_done" and event.get("task") == current:
            current = None
    return current is not None


def wait_until_busy(events_path: Path, worker_id: str, deadline: float) -> bool:
    while time.time() < deadline:
        if worker_busy(read_events(events_path), worker_id):
            return True
        time.sleep(0.05)
    return False


# ---------------------------------------------------------------------------
# Killer
# ---------------------------------------------------------------------------


def killer_loop(events_path, targets, procs, stagger, deadline, results, stop):
    for index, worker_id in enumerate(targets):
        if stop.is_set():
            return
        if index > 0:
            time.sleep(stagger)
        ready = wait_until_busy(events_path, worker_id, deadline)
        proc = procs.get(worker_id)
        if proc is None or proc.poll() is not None:
            results.append(
                {
                    "worker": worker_id,
                    "killed_at": None,
                    "trigger": "sudah berhenti",
                    "pid": None,
                }
            )
            continue
        killed_at = time.time()
        try:
            proc.kill()  # SIGKILL di POSIX, terminate di Windows
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        results.append(
            {
                "worker": worker_id,
                "killed_at": killed_at,
                "trigger": "sedang mengerjakan task" if ready else "timeout tunggu",
                "pid": proc.pid,
            }
        )
        print(f"[fault] {worker_id} dibunuh pada {killed_at:.2f} ({results[-1]['trigger']})")


# ---------------------------------------------------------------------------
# Analisis
# ---------------------------------------------------------------------------


def analyze(events, kills):
    lost_events = [event for event in events if event.get("kind") == "worker_lost"]
    kill_rows = []
    for kill in kills:
        worker_id = kill["worker"]
        detected = next(
            (event for event in lost_events if event.get("worker") == worker_id), None
        )
        detection = ""
        detected_at = ""
        reason = ""
        if detected is not None:
            detected_at = detected["ts"]
            reason = detected.get("reason", "")
            if kill.get("killed_at"):
                detection = round(detected["ts"] - kill["killed_at"], 3)
        kill_rows.append(
            {
                "worker": worker_id,
                "killed_at": kill.get("killed_at"),
                "trigger": kill.get("trigger"),
                "detected_at": detected_at,
                "detection_s": detection,
                "reason": reason,
            }
        )

    done_by_task = {
        event["task"]: event
        for event in events
        if event.get("kind") == "task_done"
    }
    first_requeue = {}
    for event in events:
        if event.get("kind") != "task_requeued":
            continue
        first_requeue.setdefault(event["task"], event)

    task_rows = []
    for task_id, requeue in first_requeue.items():
        done = done_by_task.get(task_id)
        recovery = ""
        if done is not None:
            recovery = round(done["ts"] - requeue["ts"], 3)
        task_rows.append(
            {
                "task": task_id,
                "requeue_ts": requeue["ts"],
                "attempt_at_requeue": requeue.get("attempt"),
                "reason": requeue.get("reason", ""),
                "done_ts": done["ts"] if done else "",
                "done_by": done.get("worker", "") if done else "",
                "recovery_s": recovery,
                "status": "recovered" if done else "failed",
            }
        )
    return kill_rows, task_rows


def write_csv(path: Path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Uji fault tolerance otomatis")
    parser.add_argument("--workers", type=int, default=3, help="jumlah worker lokal")
    parser.add_argument(
        "--kill",
        default="L1",
        help="daftar worker yang dibunuh, dipisah koma (mis. L1,L2)",
    )
    parser.add_argument("--stagger", type=float, default=2.0, help="jeda antar kill (detik)")
    parser.add_argument(
        "--kill-deadline",
        type=float,
        default=30.0,
        help="batas menunggu worker sibuk sebelum dibunuh (detik)",
    )
    parser.add_argument("--synth", type=int, default=200)
    parser.add_argument("--seed", type=int, default=70)
    parser.add_argument("--probe-kind", choices=["tcp", "icmp"], default="tcp")
    parser.add_argument("--probe-port", type=int, default=80)
    parser.add_argument("--count", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=0.5)
    parser.add_argument("--chunk", type=int, default=8)
    parser.add_argument("--worker-cores", type=int, default=1)
    parser.add_argument("--max-runtime", type=float, default=240.0)
    parser.add_argument("--tag", default="fault")
    parser.add_argument("--out", default="hasil")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    LOG.mkdir(parents=True, exist_ok=True)

    targets = [item.strip() for item in args.kill.split(",") if item.strip()]
    valid = {f"L{index}" for index in range(1, args.workers + 1)}
    unknown = [item for item in targets if item not in valid]
    if unknown:
        print(f"[fault] worker tidak dikenal: {unknown}. Pilihan: {sorted(valid)}")
        return 2

    port = benchmark.free_port()
    bind_host = common.interface_ip() or "127.0.0.1"
    events_path = LAPORAN / f"fault_{args.tag}_events.jsonl"
    master_log = LOG / f"fault_{args.tag}_master.log"

    python = sys.executable
    master_cmd = [
        python,
        "master.py",
        "--bind",
        bind_host,
        "--port",
        str(port),
        "--synth",
        str(args.synth),
        "--seed",
        str(args.seed),
        "--probe-kind",
        args.probe_kind,
        "--probe-port",
        str(args.probe_port),
        "--count",
        str(args.count),
        "--timeout",
        str(args.timeout),
        "--chunk",
        str(args.chunk),
        "--fairness",
        "idle",
        "--mode",
        "dynamic",
        "--expected-workers",
        str(args.workers),
        "--grace",
        "1",
        "--max-runtime",
        str(args.max_runtime),
        "--run-tag",
        args.tag,
        "--out",
        str(Path(BASE) / args.out),
        "--events",
        str(events_path),
    ]

    print(
        f"[fault] master {bind_host}:{port}, {args.workers} worker, target {args.synth}"
    )
    print(f"[fault] skenario kill: {targets} (jeda {args.stagger:.1f} s)")
    master, master_handle = benchmark.run_proc(master_cmd, master_log)

    if not benchmark.wait_port(port, timeout=15, host=bind_host):
        print("[fault] master gagal siap")
        master.terminate()
        master_handle.close()
        return 3

    workers = {}
    handles = []
    for index in range(1, args.workers + 1):
        worker_id = f"L{index}"
        cmd = [
            python,
            "worker.py",
            "--master",
            bind_host,
            "--port",
            str(port),
            "--id",
            worker_id,
            "--cores",
            str(args.worker_cores),
            "--hb-interval",
            "1.0",
            "--no-reconnect",
        ]
        proc, handle = benchmark.run_proc(cmd, LOG / f"fault_{args.tag}_{worker_id}.log")
        workers[worker_id] = proc
        handles.append(handle)

    stop = threading.Event()
    kills = []
    killer = threading.Thread(
        target=killer_loop,
        args=(
            events_path,
            targets,
            workers,
            args.stagger,
            time.time() + args.kill_deadline,
            kills,
            stop,
        ),
        daemon=True,
    )
    killer.start()

    try:
        master.wait(timeout=args.max_runtime + 60)
        rc = master.returncode
    except subprocess.TimeoutExpired:
        print("[fault] master melewati batas, dihentikan")
        master.terminate()
        rc = master.wait()
    stop.set()
    killer.join(timeout=5)

    for proc in workers.values():
        if proc.poll() is None:
            proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    for handle in handles:
        handle.close()
    master_handle.close()

    events = read_events(events_path)
    kill_rows, task_rows = analyze(events, kills)

    LAPORAN.mkdir(parents=True, exist_ok=True)
    kill_csv = LAPORAN / f"fault_{args.tag}_kills.csv"
    task_csv = LAPORAN / f"fault_{args.tag}_tasks.csv"
    write_csv(
        kill_csv,
        kill_rows,
        ["worker", "killed_at", "trigger", "detected_at", "detection_s", "reason"],
    )
    write_csv(
        task_csv,
        task_rows,
        [
            "task",
            "requeue_ts",
            "attempt_at_requeue",
            "reason",
            "done_ts",
            "done_by",
            "recovery_s",
            "status",
        ],
    )

    summary_path = Path(BASE) / args.out / f"ringkasan_{args.tag}.json"
    master_summary = {}
    if summary_path.exists():
        try:
            master_summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            master_summary = {}

    detections = [
        row["detection_s"] for row in kill_rows if isinstance(row["detection_s"], (int, float))
    ]
    recoveries = [
        row["recovery_s"] for row in task_rows if isinstance(row["recovery_s"], (int, float))
    ]
    recovered = sum(1 for row in task_rows if row["status"] == "recovered")
    failed = sum(1 for row in task_rows if row["status"] == "failed")

    report = {
        "tag": args.tag,
        "workers": args.workers,
        "kills_requested": targets,
        "kills_done": len([row for row in kill_rows if row["killed_at"]]),
        "detection_s": {
            "mean": round(statistics.mean(detections), 3) if detections else None,
            "max": round(max(detections), 3) if detections else None,
        },
        "tasks_requeued": len(task_rows),
        "tasks_recovered": recovered,
        "tasks_failed_after_retry": failed,
        "recovery_s": {
            "mean": round(statistics.mean(recoveries), 3) if recoveries else None,
            "max": round(max(recoveries), 3) if recoveries else None,
            "median": round(statistics.median(recoveries), 3) if recoveries else None,
        },
        "master_summary": {
            key: master_summary.get(key)
            for key in (
                "tasks_total",
                "tasks_done",
                "tasks_failed",
                "duration_s",
                "targets_total",
            )
        },
        "master_exit_code": rc,
    }
    report_path = LAPORAN / f"fault_{args.tag}_ringkas.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("\n" + "=" * 66)
    print(f"[fault] HASIL  tag={args.tag}")
    print("  worker dibunuh :", ", ".join(row["worker"] for row in kill_rows) or "-")
    if report["detection_s"]["mean"] is not None:
        print(
            f"  deteksi        : rata-rata {report['detection_s']['mean']:.3f} s "
            f"(maks {report['detection_s']['max']:.3f} s)"
        )
    print(f"  task requeued  : {len(task_rows)}")
    print(f"  dipulihkan     : {recovered} task, gagal {failed}")
    if report["recovery_s"]["mean"] is not None:
        print(
            f"  recovery time  : rata-rata {report['recovery_s']['mean']:.3f} s "
            f"(median {report['recovery_s']['median']:.3f} s, "
            f"maks {report['recovery_s']['max']:.3f} s)"
        )
    print(
        f"  task total     : {master_summary.get('tasks_done')} selesai / "
        f"{master_summary.get('tasks_total')} "
        f"(gagal {master_summary.get('tasks_failed')})"
    )
    print(f"  laporan        : {report_path}")
    print("=" * 66)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
