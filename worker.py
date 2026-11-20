#!/usr/bin/env python3
"""Worker Distributed Network Monitoring (Tema 13).

Worker adalah client TCP: menghubungi master, mengirim HELLO, lalu menunggu
TASK. Setiap task dipecah ke ProcessPoolExecutor sejumlah core agar benar-benar
memakai multiprocessing.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import socket
import sys
import threading
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402
import ui  # noqa: E402

def probe_batch(payload):
    """Probe satu batch target. Dijalankan di proses anak (picklable)."""
    targets, probe = payload
    rows = []
    for ip in targets:
        try:
            rows.append(common.probe_target(ip, probe))
        except Exception as exc:  # pragma: no cover - jaring pengaman
            rows.append(
                {
                    "ip": ip,
                    "status": "error",
                    "port_open": None,
                    "sent": 0,
                    "received": 0,
                    "loss_pct": 100.0,
                    "rtt_min": None,
                    "rtt_avg": None,
                    "rtt_max": None,
                    "jitter": 0.0,
                    "probe": probe.get("kind", "?"),
                    "ts": round(time.time(), 3),
                    "error": repr(exc),
                }
            )
    return rows


class Worker:
    def __init__(self, args):
        self.args = args
        self.worker_id = args.id or f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
        self.cores = max(1, args.cores or (os.cpu_count() or 1))
        self.send_lock = threading.Lock()
        self.sock: socket.socket | None = None
        self.pool = None
        self.active = 0
        self.stopping = threading.Event()
        self.health_sock: socket.socket | None = None
        self.health_port = 0
        self.lan_ip = common.interface_ip()

    # ------------------------------------------------------------------
    # Health listener: port kecil agar node ini bisa dipantau via TCP
    # ------------------------------------------------------------------

    def _start_health(self) -> None:
        wanted = max(0, int(getattr(self.args, "health_port", common.HEALTH_PORT)))
        # Utamakan bind ke IP LAN (wlp3s0) agar target TCP yang diprobe master
        # benar-benar alamat antarmuka LAN, bukan localhost/antarmuka lain.
        hosts = [self.lan_ip, "0.0.0.0"] if self.lan_ip else ["0.0.0.0"]
        ports = [wanted, 0] if wanted else [0]
        sock = None
        bind_host = "0.0.0.0"
        for port in ports:
            for host in hosts:
                sock = self._bind_health(port, host)
                if sock is not None:
                    bind_host = host
                    break
            if sock is not None:
                break
        if sock is None:
            ui.notify("WORKER", "health port gagal dibuka; node tidak bisa diprobe TCP")
            return
        self.health_sock = sock
        self.health_port = sock.getsockname()[1]
        threading.Thread(target=self._health_loop, daemon=True).start()
        shown = self.lan_ip if bind_host != "0.0.0.0" else "0.0.0.0"
        ui.notify(
            "WORKER", f"health port TCP {self.health_port} di {shown} siap diprobe"
        )

    @staticmethod
    def _bind_health(port: int, host: str = "0.0.0.0"):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
            sock.listen(64)
            sock.settimeout(1.0)
            return sock
        except OSError:
            try:
                sock.close()
            except OSError:
                pass
            return None

    def _health_loop(self) -> None:
        """Terima koneksi probe lalu tutup; tidak ada data yang dipertukarkan."""
        while not self.stopping.is_set():
            sock = self.health_sock
            if sock is None:
                return
            try:
                conn, _addr = sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                conn.close()
            except OSError:
                pass

    def _stop_health(self) -> None:
        sock = self.health_sock
        self.health_sock = None
        if sock is None:
            return
        try:
            sock.close()
        except OSError:
            pass

    # ------------------------------------------------------------------

    def run(self) -> None:
        self._start_health()
        backoff = 1.0
        while not self.stopping.is_set():
            clean = False
            try:
                clean = self._session()
            except (OSError, ValueError, ConnectionError) as exc:
                ui.notify("WORKER", f"koneksi gagal: {exc}")
            self.sock = None

            if clean or not self.args.reconnect or self.stopping.is_set():
                break

            ui.notify("WORKER", f"mencoba lagi dalam {backoff:.0f} detik ...")
            if self.stopping.wait(backoff):
                break
            backoff = min(8.0, backoff * 2)

        self._stop_health()
        ui.notify("WORKER", "berhenti")

    def _session(self) -> bool:
        args = self.args
        ui.notify("WORKER", f"menyambung ke {args.master}:{args.port} ...")
        sock = socket.create_connection((args.master, args.port), timeout=5.0)
        sock.settimeout(None)
        common.set_keepalive(sock)
        self.sock = sock
        self.send_lock = threading.Lock()

        common.send_msg(
            sock,
            {
                "type": "HELLO",
                "worker_id": self.worker_id,
                "hostname": socket.gethostname(),
                "cores": self.cores,
                "health_port": self.health_port,
                "lan_ip": self.lan_ip,
                "version": common.PROTO_VERSION,
            },
        )

        welcome = common.recv_msg(sock)
        if not welcome or welcome.get("type") != "WELCOME":
            raise ConnectionError("WELCOME tidak diterima dari master")

        ui.notify(
            "WORKER",
            f"TERHUBUNG ke master {args.master}:{args.port} sebagai "
            f"{self.worker_id} ({self.cores} core, IP {self.lan_ip or '?'}), "
            f"chunk={welcome.get('chunk_size') or 'auto'}, "
            f"health={self.health_port or '-'}",
        )

        heartbeat_stop = threading.Event()
        heartbeat = threading.Thread(
            target=self._heartbeat,
            args=(sock, self.send_lock, heartbeat_stop),
            daemon=True,
        )
        heartbeat.start()

        self.pool = None
        if self.args.inline:
            ui.notify("WORKER", "mode inline: probe dijalankan di proses utama")
        else:
            try:
                self.pool = self._make_pool()
            except Exception as exc:  # pragma: no cover - Termux/Android
                ui.notify(
                    "WORKER",
                    f"multiprocessing tidak tersedia ({exc}); memakai mode inline",
                )
                self.pool = None

        if self.pool is not None:
            # Panaskan proses anak saat fase discovery, supaya task pertama
            # tidak menanggung biaya start forkserver (bikin distribusi timpang).
            try:
                self.pool.submit(int, 0).result(timeout=10)
            except Exception as exc:  # pragma: no cover - Termux/Android
                ui.notify("WORKER", f"prewarm proses gagal ({exc}); beralih inline")
                try:
                    self.pool.shutdown(wait=False)
                except Exception:
                    pass
                self.pool = None

        clean = False
        try:
            while not self.stopping.is_set():
                msg = common.recv_msg(sock)
                if msg is None:
                    ui.notify("WORKER", "master menutup koneksi")
                    break
                mtype = msg.get("type")
                if mtype == "TASK":
                    self._run_task(msg)
                elif mtype in ("DRAIN", "BYE"):
                    ui.notify("WORKER", f"menerima {mtype}, keluar dengan rapi")
                    clean = True
                    break
        finally:
            if self.pool is not None:
                self.pool.shutdown(wait=False)
                self.pool = None
            heartbeat_stop.set()
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass
        return clean

    def _make_pool(self) -> ProcessPoolExecutor:
        """Buat pool proses tanpa mewarisi socket master.

        Dengan `fork`, proses anak mewarisi file descriptor socket sehingga
        koneksi baru benar-benar tertutup ketika worker dibunuh (master harus
        menunggu heartbeat timeout). `forkserver`/`spawn` tidak mewarisi fd,
        jadi deteksi kegagalan menjadi instan.
        """
        available = mp.get_all_start_methods()
        method = "forkserver" if "forkserver" in available else "spawn"
        context = mp.get_context(method)
        return ProcessPoolExecutor(max_workers=self.cores, mp_context=context)

    def _heartbeat(self, sock, lock, stop: threading.Event) -> None:
        interval = max(0.5, self.args.hb_interval)
        while not stop.wait(interval):
            payload = {
                "type": "HEARTBEAT",
                "worker_id": self.worker_id,
                "ts": time.time(),
            }
            try:
                common.send_msg(sock, payload, lock)
            except (OSError, ValueError):
                return

    def _run_task(self, msg: dict) -> None:
        task_id = msg.get("task_id", "?")
        targets = msg.get("targets") or []
        probe = msg.get("probe") or {}

        started = time.perf_counter()
        self.active += 1
        rows = []
        errors = []
        try:
            parts = common.split_evenly(targets, self.cores)
            payloads = [(part, probe) for part in parts]
            if self.pool is not None:
                try:
                    for part_rows in self.pool.map(
                        probe_batch, payloads, chunksize=1
                    ):
                        rows.extend(part_rows)
                except Exception as exc:  # BrokenProcessPool / batasan Android
                    ui.notify(
                        "WORKER",
                        f"pool proses gagal ({exc}); beralih ke mode inline",
                    )
                    try:
                        self.pool.shutdown(wait=False)
                    except Exception:
                        pass
                    self.pool = None
            if self.pool is None:
                rows = []
                for part in parts:
                    rows.extend(probe_batch((part, probe)))
        except Exception as exc:  # pragma: no cover - jaring pengaman
            errors.append(repr(exc))
        finally:
            self.active -= 1

        duration_ms = (time.perf_counter() - started) * 1000.0
        message = {
            "type": "RESULT",
            "task_id": task_id,
            "worker_id": self.worker_id,
            "duration_ms": round(duration_ms, 1),
            "rows": rows,
            "errors": errors,
        }
        common.send_msg(self.sock, message, self.send_lock)

        up = sum(1 for row in rows if row.get("status") == "up")
        ui.notify(
            "TASK",
            f"{task_id}: {len(rows)} target ({up} up), {duration_ms:.0f} ms",
        )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Worker monitoring jaringan (TCP)")
    parser.add_argument(
        "--master",
        default=common.default_master_ip(),
        help=f"IP master (default {common.default_master_ip()})",
    )
    parser.add_argument("--port", type=int, default=common.CONTROL_PORT)
    parser.add_argument("--id", default="", help="ID worker (default: hostname-acak)")
    parser.add_argument(
        "--cores",
        type=int,
        default=0,
        help="batas proses anak (default: semua core)",
    )
    parser.add_argument(
        "--hb-interval",
        type=float,
        default=2.0,
        help="interval heartbeat detik",
    )
    parser.add_argument(
        "--no-reconnect",
        action="store_false",
        dest="reconnect",
        help="jangan sambung ulang bila koneksi putus",
    )
    parser.add_argument(
        "--health-port",
        type=int,
        default=common.HEALTH_PORT,
        help=f"port health untuk diprobe master (default {common.HEALTH_PORT}, 0=acak)",
    )
    parser.add_argument(
        "--inline",
        action="store_true",
        help="jalankan probe di proses utama (fallback untuk Termux/Android)",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="tampilkan form parameter (opsional; default worker langsung jalan)",
    )
    return parser.parse_args(argv)


def run_interactive(args):
    fields = [
        ui.Field("master", "Alamat master", args.master),
        ui.Field("port", "Port TCP", args.port, "int", minimum=1),
        ui.Field("id", "ID worker (kosong=auto)", args.id),
        ui.Field("cores", "Jumlah core (0=auto)", args.cores, "int", minimum=0),
        ui.Field(
            "health_port",
            "Port health (0=acak)",
            args.health_port,
            "int",
            minimum=0,
        ),
        ui.Field(
            "hb_interval", "Interval heartbeat", args.hb_interval, "float", minimum=0.5
        ),
        ui.Field("reconnect", "Sambung ulang", args.reconnect, "bool"),
        ui.Field("inline", "Mode inline (tanpa proses)", args.inline, "bool"),
    ]
    values = ui.run_form(
        "WORKER - Distributed Network Monitoring",
        fields,
        "Tekan nomor untuk mengubah, Enter untuk mulai",
    )
    for key, value in values.items():
        setattr(args, key, value)
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    # Worker sengaja tidak memunculkan form otomatis: `python worker.py` harus
    # langsung menyambung ke master. Form hanya jika diminta eksplisit.
    if args.interactive:
        args = run_interactive(args)

    worker = Worker(args)

    def handle_sigint(_signum, _frame):
        print("\n[worker] dihentikan pengguna")
        worker.stopping.set()

    try:
        import signal

        signal.signal(signal.SIGINT, handle_sigint)
        signal.signal(signal.SIGTERM, handle_sigint)
    except (ImportError, ValueError):  # pragma: no cover
        pass

    worker.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
