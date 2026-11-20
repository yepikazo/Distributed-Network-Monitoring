#!/usr/bin/env python3
"""Orkestrasi pengujian Tema 13.

Perintah:
  python benchmark.py local    # sequential + uji lokal dengan 1..K worker
  python benchmark.py report   # gabungkan hasil/ menjadi tabel + grafik
  python benchmark.py plan     # cetak panduan uji 10 node fisik

Catatan: `local` menjalankan master dan worker sebagai proses terpisah di satu
komputer. Angka ini untuk memverifikasi protokol dan memberi gambaran awal;
angka resmi laporan harus berasal dari 10 node fisik (`plan`).
"""

from __future__ import annotations

import argparse
import csv
import json
import socket
import subprocess
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))
import common  # noqa: E402

HASIL = BASE / "hasil"
LAPORAN = BASE / "laporan"
LOG = BASE / "laporan" / "log"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_port(port: int, timeout: float = 15.0, host: str = "127.0.0.1") -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.3):
                return True
        except OSError:
            time.sleep(0.15)
    return False


def run_proc(cmd, log_path: Path):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handle = log_path.open("w", encoding="utf-8")
    return subprocess.Popen(
        cmd,
        cwd=str(BASE),
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
    ), handle


def common_probe_args(args):
    return [
        "--synth",
        str(args.synth),
        "--seed",
        str(args.seed),
        "--probe-port",
        str(args.probe_port),
        "--count",
        str(args.count),
        "--timeout",
        str(args.timeout),
    ]


# ---------------------------------------------------------------------------
# local
# ---------------------------------------------------------------------------


def cmd_local(args) -> int:
    LOG.mkdir(parents=True, exist_ok=True)
    probe = common_probe_args(args)
    python = sys.executable

    print(f"[bench] baseline sequential ({args.synth} target) ...")
    tag = "sequential"
    cmd = [
        python,
        "baseline.py",
        "--mode",
        "sequential",
        "--run-tag",
        tag,
        "--out",
        str(HASIL),
    ] + probe
    code = subprocess.call(cmd, cwd=str(BASE))
    if code != 0:
        print("[bench] baseline gagal")
        return code

    for workers in args.workers:
        port = free_port()
        tag = f"local_k{workers}"
        bind_host = common.interface_ip() or "127.0.0.1"
        print(f"\n[bench] uji lokal: {workers} worker (port {port}, host {bind_host}) ...")
        master_cmd = [
            python,
            "master.py",
            "--bind",
            bind_host,
            "--port",
            str(port),
            "--run-tag",
            tag,
            "--mode",
            "dynamic",
            "--chunk",
            str(args.chunk),
            "--fairness",
            "idle",
            "--expected-workers",
            str(workers),
            "--out",
            str(HASIL),
            "--max-runtime",
            str(args.max_runtime),
            "--grace",
            "1",
        ] + probe
        master, master_log = run_proc(master_cmd, LOG / f"{tag}_master.log")

        if not wait_port(port, timeout=15, host=bind_host):
            print("[bench] master tidak siap, dilewati")
            master.terminate()
            master_log.close()
            continue

        workers_proc = []
        for index in range(1, workers + 1):
            worker_cmd = [
                python,
                "worker.py",
                "--master",
                bind_host,
                "--port",
                str(port),
                "--id",
                f"L{index}",
                "--cores",
                str(args.worker_cores),
                "--hb-interval",
                "1.0",
                "--no-reconnect",
            ]
            proc, handle = run_proc(
                worker_cmd, LOG / f"{tag}_worker{index}.log"
            )
            workers_proc.append((proc, handle))

        # tunggu master selesai; master keluar sendiri setelah semua task selesai
        try:
            master.wait(timeout=args.max_runtime)
        except subprocess.TimeoutExpired:
            print("[bench] master melewati batas waktu, dihentikan")
            master.terminate()

        for proc, handle in workers_proc:
            if proc.poll() is None:
                proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            handle.close()
        master_log.close()

    print("\n[bench] selesai. Jalankan `python benchmark.py report`.")
    return 0


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------


def load_summaries():
    items = []
    if not HASIL.exists():
        return items
    for path in sorted(HASIL.glob("ringkasan_*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        data["_path"] = str(path)
        items.append(data)
    return items


def is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def summarize_table(items):
    baseline = next((i for i in items if i.get("run_tag") == "sequential"), None)
    base_duration = baseline.get("duration_s") if baseline else None

    rows = []
    for item in items:
        workers = len(item.get("workers_seen") or [])
        duration = item.get("duration_s")
        speedup = None
        efficiency = None
        if base_duration and is_number(duration) and duration > 0:
            speedup = base_duration / duration
            if workers > 0:
                efficiency = speedup / workers
        rows.append(
            {
                "run_tag": item.get("run_tag"),
                "mode": item.get("mode"),
                "workers": workers,
                "targets": item.get("targets_total"),
                "tasks_done": item.get("tasks_done"),
                "tasks_failed": item.get("tasks_failed", 0),
                "duration_s": duration,
                "throughput": item.get("throughput_targets_s"),
                "speedup": round(speedup, 2) if speedup else "",
                "efficiency": round(efficiency, 3) if efficiency else "",
                "imbalance": item.get("imbalance_ratio") if item.get("imbalance_ratio") else "",
            }
        )
    return rows


def write_report(rows):
    LAPORAN.mkdir(parents=True, exist_ok=True)
    csv_path = LAPORAN / "hasil_gabungan.csv"
    fields = [
        "run_tag",
        "mode",
        "workers",
        "targets",
        "tasks_done",
        "tasks_failed",
        "duration_s",
        "throughput",
        "speedup",
        "efficiency",
        "imbalance",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    lines = [
        "# Tabel Hasil Pengujian",
        "",
        "| Run | Mode | Worker | Target | Selesai | Gagal | Waktu (s) | Throughput | Speedup | Efficiency | Imbalance |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            "| {run_tag} | {mode} | {workers} | {targets} | {tasks_done} | {tasks_failed} "
            "| {duration_s} | {throughput} | {speedup} | {efficiency} | {imbalance} |".format(
                **row
            )
        )
    (LAPORAN / "tabel_hasil.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"[report] tabel  : {LAPORAN / 'tabel_hasil.md'}")
    print(f"[report] csv    : {csv_path}")

    try:
        make_charts(rows)
    except Exception as exc:  # pragma: no cover
        print(f"[report] grafik dilewati: {exc}")


def make_charts(rows):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    series = [
        row
        for row in rows
        if row["workers"] and is_number(row["duration_s"])
    ]
    if not series:
        return

    series.sort(key=lambda row: (row["mode"] or "", row["workers"]))

    plt.figure(figsize=(7, 4.2))
    for mode in sorted({row["mode"] for row in series}):
        subset = [row for row in series if row["mode"] == mode]
        plt.plot(
            [row["workers"] for row in subset],
            [row["duration_s"] for row in subset],
            marker="o",
            label=mode,
        )
    plt.xlabel("Jumlah worker")
    plt.ylabel("Waktu eksekusi (detik)")
    plt.title("Waktu vs jumlah worker")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(LAPORAN / "grafik_waktu_vs_worker.png", dpi=150)
    plt.close()

    speed = [row for row in series if is_number(row["speedup"])]
    if speed:
        plt.figure(figsize=(7, 4.2))
        for mode in sorted({row["mode"] for row in speed}):
            subset = [row for row in speed if row["mode"] == mode]
            plt.plot(
                [row["workers"] for row in subset],
                [row["speedup"] for row in subset],
                marker="o",
                label=f"{mode} (aktual)",
            )
        max_workers = max(row["workers"] for row in speed)
        plt.plot(
            [1, max_workers],
            [1, max_workers],
            linestyle="--",
            color="gray",
            label="ideal",
        )
        plt.xlabel("Jumlah worker")
        plt.ylabel("Speedup")
        plt.title("Speedup vs jumlah worker")
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        plt.savefig(LAPORAN / "grafik_speedup_vs_worker.png", dpi=150)
        plt.close()

    print(f"[report] grafik : {LAPORAN / 'grafik_waktu_vs_worker.png'}")


def cmd_report(_args) -> int:
    items = load_summaries()
    if not items:
        print("[report] belum ada hasil di folder hasil/")
        return 1
    rows = summarize_table(items)
    write_report(rows)

    print("\n" + "=" * 78)
    header = f"{'run':<18}{'mode':<10}{'worker':>7}{'waktu(s)':>10}{'speedup':>9}{'eff':>7}{'imbalance':>11}"
    print(header)
    print("-" * 78)
    for row in rows:
        print(
            f"{str(row['run_tag']):<18}{str(row['mode']):<10}"
            f"{row['workers']:>7}{str(row['duration_s']):>10}"
            f"{str(row['speedup']):>9}{str(row['efficiency']):>7}"
            f"{str(row['imbalance']):>11}"
        )
    print("=" * 78)
    return 0


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


def cmd_plan(_args) -> int:
    print(
        """
Panduan uji 10 node fisik (1 master + 9 worker)
===============================================

1) Di Master (mis. 10.199.101.109) buka port kontrol:
     sudo ufw allow 9998/tcp
     (atau pastikan firewall lab mengizinkan TCP 9998)

3) Sequential baseline (1 proses, target sama):
     python baseline.py --mode sequential --synth 500 --run-tag sequential

4) Demo monitoring node (target = IP 9 worker):
     python master.py --expected-workers 9 --repeat 5 \\
         --fairness count --run-tag dist_ipworker_k9 --out hasil

   Uji performa/skalabilitas (target sama untuk 1..9 node):
     python master.py --synth 500 --expected-workers 9 --mode dynamic \\
         --chunk 25 --fairness idle --run-tag dist_synth_k9 --out hasil

5) Di setiap worker (buka health port + ganti IP master bila perlu):
     sudo ufw allow 9997/tcp
     python worker.py --master 10.199.101.109 --port 9998 --cores 4

6) Variasi skenario:
     scalability   : ulangi langkah 4-5 dengan 1, 2, 4, 6, 9 worker
                     dan ganti --expected-workers + --run-tag
     load balancing: bandingkan --fairness count vs idle/off,
                     atau --mode static vs dynamic
     fault tolerance: matikan 1-2 worker saat sistem berjalan; amati log master
                      (task requeue) dan catat recovery time
     local parallel: python benchmark.py local   # alternatif 1 mesin

7) Gabungkan hasil:
     python benchmark.py report
"""
    )
    return 0


# ---------------------------------------------------------------------------


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Benchmark Tema 13")
    sub = parser.add_subparsers(dest="command", required=True)

    local = sub.add_parser("local", help="sequential + uji lokal beberapa worker")
    local.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    local.add_argument("--synth", type=int, default=60)
    local.add_argument("--seed", type=int, default=70)
    local.add_argument("--probe-port", type=int, default=80)
    local.add_argument("--count", type=int, default=1)
    local.add_argument("--timeout", type=float, default=0.3)
    local.add_argument("--chunk", type=int, default=10)
    local.add_argument("--worker-cores", type=int, default=1)
    local.add_argument("--max-runtime", type=float, default=300.0)

    sub.add_parser("report", help="gabungkan hasil menjadi tabel/grafik")
    sub.add_parser("plan", help="cetak panduan 10 node")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.command == "local":
        return cmd_local(args)
    if args.command == "report":
        return cmd_report(args)
    if args.command == "plan":
        return cmd_plan(args)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
