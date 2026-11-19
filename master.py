#!/usr/bin/env python3
"""Master Distributed Network Monitoring (Tema 13).

Master adalah server TCP di port kontrol (default 9998). Worker menghubungi
master, lalu master membagikan task probe secara dinamis (pull model): satu
task in-flight per worker. Task yang timeout atau worker yang mati akan
dikembalikan ke antrean (retry/requeue).
"""

from __future__ import annotations

import argparse
import json
import math
import queue
import socket
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from itertools import count
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402
import ui  # noqa: E402

EOF = object()


@dataclass
class WorkerInfo:
    worker_id: str
    hostname: str
    cores: int
    sock: socket.socket
    send_lock: threading.Lock
    inbox: queue.Queue
    ip: str = ""
    health_port: int = 0
    peer: str = ""
    state: str = "READY"  # READY | ASSIGNED
    last_hb: float = field(default_factory=time.time)
    inflight: str | None = None
    deadline: float = 0.0
    chunks_done: int = 0
    rows_done: int = 0
    busy_s: float = 0.0
    load: object = None
    cpu_pct: object = None
    mem_pct: object = None
    connected_at: float = field(default_factory=time.time)
    alive: bool = True


@dataclass
class TaskRecord:
    task_id: str
    targets: list
    probe: dict
    attempt: int = 0
    status: str = "QUEUED"  # QUEUED | RUNNING | DONE | FAILED
    worker: str | None = None
    started: float = 0.0
    deadline: float = 0.0
    finished: float = 0.0
    priority: int = 1


class Master:
    def __init__(self, args):
        self.args = args
        self.lock = threading.RLock()
        self.tasks: queue.PriorityQueue = queue.PriorityQueue()
        self.ledger: dict[str, TaskRecord] = {}
        self.workers: dict[str, WorkerInfo] = {}
        self.worker_history: dict[str, WorkerInfo] = {}
        self.seen_workers: set[str] = set()
        self.rows: list[dict] = []
        self.failures: list[dict] = []
        self.stop = threading.Event()
        self.seq = count()
        self.total_tasks = 0
        self.target_total = 0
        self.finished_tasks = 0
        self.failed_tasks = 0
        self.start_time = time.time()
        self.done_since: float | None = None
        self.server: socket.socket | None = None
        self.required_workers = max(0, int(getattr(args, "expected_workers", 0) or 0))
        self.effective_chunk = 0
        self.target_source_used = ""
        self.targets_built = False
        self.events_path = Path(args.events) if getattr(args, "events", "") else None
        self.event_lock = threading.Lock()
        if self.events_path:
            self.events_path.parent.mkdir(parents=True, exist_ok=True)
            self.events_path.write_text("", encoding="utf-8")

    # ------------------------------------------------------------------
    # Event terstruktur (JSONL) untuk analisis fault tolerance
    # ------------------------------------------------------------------

    def _event(self, kind: str, **fields) -> None:
        if not self.events_path:
            return
        payload = {"ts": round(time.time(), 4), "kind": kind}
        payload.update(fields)
        line = json.dumps(payload, ensure_ascii=False)
        with self.event_lock:
            with self.events_path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")

    # ------------------------------------------------------------------
    # Persiapan task
    # ------------------------------------------------------------------

    def defers_targets(self) -> bool:
        """True jika target baru bisa disusun setelah worker terhubung."""
        return not self.args.synth and self.args.target_source in ("workers", "both")

    def _worker_targets(self) -> list:
        """Ambil IP worker yang hidup sebagai target monitoring.

        Probe TCP memakai health port yang dilaporkan worker pada HELLO.
        """
        with self.lock:
            workers = [w for w in self.workers.values() if w.alive]
        entries = []
        seen = set()
        for worker in sorted(workers, key=lambda w: (w.ip, w.health_port, w.worker_id)):
            if not worker.ip:
                continue
            if self.args.probe_kind == "tcp":
                if not worker.health_port:
                    ui.notify(
                        "MASTER",
                        f"{worker.worker_id} tidak membuka health port; dilewati",
                    )
                    continue
                entry = f"{worker.ip}:{worker.health_port}"
            else:
                entry = worker.ip
            if entry not in seen:
                seen.add(entry)
                entries.append(entry)
        return entries

    def _collect_targets(self):
        args = self.args
        if args.synth:
            return (
                common.synth_targets(args.synth, seed=args.seed),
                f"sintetis:{args.synth}",
            )

        targets = []
        if args.target_source in ("file", "both"):
            path = args.targets or "targets.txt"
            targets.extend(
                common.load_targets(path, repeat=args.repeat, limit=args.limit)
            )
        if args.target_source in ("workers", "both"):
            worker_targets = self._worker_targets()
            repeat = max(1, args.repeat) if args.target_source == "workers" else 1
            for _ in range(repeat):
                targets.extend(worker_targets)
        if args.limit and args.limit > 0 and args.target_source == "workers":
            targets = targets[: args.limit]

        if args.target_source == "workers":
            label = "ip-worker"
        elif args.target_source == "both":
            label = "file+ip-worker"
        else:
            label = f"file:{args.targets or 'targets.txt'}"
        return targets, label

    def build_tasks(self) -> bool:
        args = self.args
        targets, source = self._collect_targets()
        if not targets:
            ui.notify(
                "MASTER",
                "tidak ada target: pastikan worker terhubung atau isi targets.txt",
            )
            return False

        self.target_total = len(targets)
        self.target_source_used = source

        if args.mode == "static":
            expected = max(1, args.expected_workers or 9)
            chunk = max(1, math.ceil(len(targets) / expected))
        elif args.chunk and args.chunk > 0:
            chunk = max(1, args.chunk)
        elif source == "ip-worker":
            chunk = 1
        else:
            chunk = 50
        self.effective_chunk = chunk

        probe = {
            "kind": args.probe_kind,
            "port": args.probe_port,
            "count": args.count,
            "timeout": args.timeout,
        }

        with self.lock:
            for start in range(0, len(targets), chunk):
                part = targets[start : start + chunk]
                task_id = f"T{len(self.ledger) + 1:04d}-{uuid.uuid4().hex[:6]}"
                record = TaskRecord(task_id=task_id, targets=part, probe=probe)
                self.ledger[task_id] = record
                self.tasks.put((1, next(self.seq), task_id))

            self.total_tasks = len(self.ledger)
            self.targets_built = True

        print(f"[master] sumber target: {source} ({len(targets)} target)")
        print(
            f"[master] {len(targets)} target -> {self.total_tasks} task "
            f"(mode={args.mode}, chunk={chunk})"
        )
        self._event(
            "plan",
            tasks_total=self.total_tasks,
            targets_total=len(targets),
            mode=args.mode,
            chunk=chunk,
            target_source=source,
            fairness=getattr(args, "fairness", "count"),
        )
        return True

    # ------------------------------------------------------------------
    # Server
    # ------------------------------------------------------------------

    def serve(self) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        bind_ip = self.args.bind
        try:
            server.bind((bind_ip, self.args.port))
        except OSError as exc:
            if bind_ip in ("", "0.0.0.0"):
                raise
            # Antarmuka LAN bisa belum siap (mis. Wi-Fi baru tersambung).
            # Tetap layani semua antarmuka agar master tidak gagal start.
            ui.notify(
                "MASTER",
                f"bind ke {bind_ip} gagal ({exc}); memakai 0.0.0.0",
            )
            server.close()
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(("0.0.0.0", self.args.port))
            bind_ip = "0.0.0.0"
        server.listen(64)
        server.settimeout(0.5)
        self.server = server
        print(
            f"[master] mendengarkan TCP {bind_ip}:{self.args.port} "
            f"(harap buka port ini di firewall)"
        )
        print(
            f"[master] butuh {self.args.expected_workers or '-'} worker; "
            f"Ctrl+C untuk berhenti"
        )
        threading.Thread(target=self._status_loop, daemon=True).start()
        if self.defers_targets():
            threading.Thread(target=self._prepare_loop, daemon=True).start()

        try:
            while not self.stop.is_set():
                try:
                    conn, addr = server.accept()
                except socket.timeout:
                    self._maybe_finish()
                    if (
                        self.args.max_runtime
                        and time.time() - self.start_time > self.args.max_runtime
                    ):
                        print("[master] batas waktu maksimum tercapai, berhenti")
                        self.stop.set()
                    continue
                except OSError:
                    break
                threading.Thread(
                    target=self._handle_conn, args=(conn, addr), daemon=True
                ).start()
        except KeyboardInterrupt:
            print("\n[master] dihentikan pengguna")
            self.stop.set()
        finally:
            try:
                server.close()
            except OSError:
                pass
            self._shutdown_workers()
            self.finalize()

    # ------------------------------------------------------------------
    # Penemuan target dari node yang terhubung
    # ------------------------------------------------------------------

    def _prepare_loop(self) -> None:
        """Tunggu worker terhubung, lalu susun target dari IP mereka."""
        args = self.args
        need = max(0, int(args.expected_workers or 0))
        wait = max(0.0, float(args.discovery_timeout))
        deadline = time.time() + wait
        first_seen = None
        ui.notify(
            "MASTER",
            f"menunggu node terhubung (butuh {need or 1}, maks {wait:.0f} detik) ...",
        )

        while not self.stop.is_set():
            with self.lock:
                connected = len(self.workers)
            if need:
                if connected >= need:
                    break
            else:
                # Tanpa --expected-workers: kumpulkan node sebentar setelah
                # node pertama, agar worker yang menyusul tidak terlewat.
                if connected >= 1:
                    if first_seen is None:
                        first_seen = time.time()
                    if time.time() - first_seen >= 2.0:
                        break
            if time.time() >= deadline:
                break
            time.sleep(0.1)

        with self.lock:
            connected = len(self.workers)
            if need and connected < need:
                # Jangan menggantung: cukupkan dengan node yang benar-benar ada.
                self.required_workers = max(1, connected)

        if connected == 0:
            ui.notify("MASTER", "tidak ada worker yang terhubung; dibatalkan")
            self.stop.set()
            return

        if need and connected < need:
            ui.notify(
                "MASTER",
                f"hanya {connected}/{need} node terhubung; lanjut dengan {connected} node",
            )
        ui.notify("MASTER", f"{connected} node siap; menyusun target dari IP worker")
        if not self.build_tasks():
            self.stop.set()

    # ------------------------------------------------------------------
    # Koneksi worker
    # ------------------------------------------------------------------

    def _handle_conn(self, conn: socket.socket, addr) -> None:
        peer = f"{addr[0]}:{addr[1]}"
        conn.settimeout(10.0)
        try:
            hello = common.recv_msg(conn)
        except (OSError, ValueError):
            try:
                conn.close()
            except OSError:
                pass
            return

        if not hello or hello.get("type") != "HELLO":
            try:
                common.send_msg(conn, {"type": "ERROR", "reason": "HELLO diharapkan"})
            except OSError:
                pass
            try:
                conn.close()
            except OSError:
                pass
            return

        conn.settimeout(None)
        common.set_keepalive(conn)

        worker_id = hello.get("worker_id") or f"W-{uuid.uuid4().hex[:6]}"
        hostname = hello.get("hostname") or peer
        cores = max(1, int(hello.get("cores") or 1))
        health_port = max(0, int(hello.get("health_port") or 0))

        # Worker melaporkan IP antarmuka LAN-nya (wlp3s0). Fallback ke IP peer
        # bila worker lama/tidak mengirim lan_ip.
        worker_ip = addr[0]
        reported_ip = hello.get("lan_ip")
        if isinstance(reported_ip, str) and reported_ip.strip():
            candidate = reported_ip.strip()
            try:
                socket.inet_aton(candidate)
                worker_ip = candidate
            except OSError:
                pass

        old = self.workers.get(worker_id)
        if old is not None:
            self._worker_gone(old, "digantikan koneksi baru")

        worker = WorkerInfo(
            worker_id=worker_id,
            hostname=hostname,
            cores=cores,
            sock=conn,
            send_lock=threading.Lock(),
            inbox=queue.Queue(),
            ip=worker_ip,
            health_port=health_port,
            peer=peer,
        )
        with self.lock:
            self.workers[worker_id] = worker
            self.worker_history[worker_id] = worker
            self.seen_workers.add(worker_id)

        with self.lock:
            total = len(self.workers)
        expected = self.args.expected_workers
        ui.notify(
            "NODE+",
            f"{worker_id} ({hostname}, {cores} core) IP {worker_ip} dari {peer} | "
            f"total {total}/{expected or '?'}",
        )
        self._event(
            "worker_join",
            worker=worker_id,
            hostname=hostname,
            cores=cores,
            peer=peer,
            health_port=health_port,
        )

        welcome = {
            "type": "WELCOME",
            "session_id": uuid.uuid4().hex[:8],
            "server_time": time.time(),
            "heartbeat_ms": int(self.args.hb_timeout * 1000 / 3),
            "task_timeout_ms": int(self.args.task_timeout_cap * 1000),
            "chunk_size": self.effective_chunk or self.args.chunk,
        }
        if not self._send(worker, welcome):
            self._worker_gone(worker, "gagal mengirim WELCOME")
            return

        threading.Thread(
            target=self._reader, args=(conn, worker.inbox), daemon=True
        ).start()
        self._worker_loop(worker)

    def _reader(self, conn: socket.socket, inbox: queue.Queue) -> None:
        try:
            while True:
                msg = common.recv_msg(conn)
                if msg is None:
                    break
                inbox.put(msg)
        except (OSError, ValueError):
            pass
        finally:
            inbox.put(EOF)
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                conn.close()
            except OSError:
                pass

    def _worker_loop(self, worker: WorkerInfo) -> None:
        try:
            self._assign(worker)
            while not self.stop.is_set() and worker.alive:
                try:
                    msg = worker.inbox.get(timeout=0.1)
                except queue.Empty:
                    msg = None

                if msg is EOF:
                    self._worker_gone(worker, "koneksi ditutup")
                    return

                if msg is None:
                    now = time.time()
                    if worker.inflight:
                        record = self.ledger.get(worker.inflight)
                        if (
                            record is not None
                            and record.status == "RUNNING"
                            and now > record.deadline
                        ):
                            self._requeue(worker, record, "timeout")
                    if now - worker.last_hb > self.args.hb_timeout:
                        self._worker_gone(worker, "heartbeat timeout")
                        return
                    self._assign(worker)
                    self._maybe_finish()
                    continue

                mtype = msg.get("type")
                if mtype == "HEARTBEAT":
                    worker.last_hb = time.time()
                    worker.load = msg.get("load")
                    worker.cpu_pct = msg.get("cpu_pct")
                    worker.mem_pct = msg.get("mem_pct")
                elif mtype == "RESULT":
                    self._on_result(worker, msg)
                elif mtype == "BYE":
                    self._worker_gone(worker, "BYE")
                    return
        except Exception as exc:  # pragma: no cover - jaring pengaman
            print(f"[master] error pada worker {worker.worker_id}: {exc!r}")
            self._worker_gone(worker, f"error: {exc!r}")

    # ------------------------------------------------------------------
    # Penjadwalan
    # ------------------------------------------------------------------

    def _estimate_timeout(self, record: TaskRecord, worker: WorkerInfo) -> float:
        probe = record.probe
        per_core = math.ceil(len(record.targets) / max(1, worker.cores))
        count = max(1, int(probe.get("count", 3)))
        timeout = float(probe.get("timeout", 0.8))
        estimate = per_core * count * timeout + float(self.args.slack)
        return min(self.args.task_timeout_cap, max(5.0, estimate))

    def _assign(self, worker: WorkerInfo) -> None:
        while True:
            if (
                self.stop.is_set()
                or not worker.alive
                or worker.state != "READY"
            ):
                return

            # Fairness dapat diatur:
            # count = seimbangkan jumlah task (termasuk in-flight)
            # idle  = hanya dahulukan node READY yang tertinggal (work-conserving)
            # off   = murni siapa cepat dia dapat
            fairness = getattr(self.args, "fairness", "count")
            if fairness != "off":
                if fairness == "count":
                    now = time.time()
                    with self.lock:
                        scores = [
                            w.chunks_done + (1 if w.inflight else 0)
                            for w in self.workers.values()
                            if w.alive and now - w.last_hb <= self.args.hb_timeout
                        ]
                    score = worker.chunks_done + (1 if worker.inflight else 0)
                    if len(scores) > 1 and score > min(scores):
                        return
                else:
                    with self.lock:
                        ready = [
                            w
                            for w in self.workers.values()
                            if w.alive and w.state == "READY"
                        ]
                    if len(ready) > 1 and worker.chunks_done > min(
                        w.chunks_done for w in ready
                    ):
                        return

            try:
                _priority, _seq, task_id = self.tasks.get_nowait()
            except queue.Empty:
                self._maybe_finish()
                return

            record = self.ledger.get(task_id)
            if record is None or record.status != "QUEUED":
                continue  # entri basi (task sudah selesai/gagal)

            now = time.time()
            record.attempt += 1
            record.status = "RUNNING"
            record.worker = worker.worker_id
            record.started = now
            record.deadline = now + self._estimate_timeout(record, worker)

            worker.inflight = task_id
            worker.state = "ASSIGNED"
            self._event(
                "task_assigned",
                task=task_id,
                worker=worker.worker_id,
                attempt=record.attempt,
                targets=len(record.targets),
            )

            message = {
                "type": "TASK",
                "task_id": task_id,
                "targets": record.targets,
                "probe": record.probe,
                "attempt": record.attempt,
            }
            if not self._send(worker, message):
                self._requeue(worker, record, "gagal mengirim task")
                return
            return

    def _on_result(self, worker: WorkerInfo, msg: dict) -> None:
        task_id = msg.get("task_id")
        rows = msg.get("rows") or []
        duration = float(msg.get("duration_ms") or 0.0) / 1000.0

        with self.lock:
            record = self.ledger.get(task_id)
            stale = record is None or record.status in ("DONE", "FAILED")
            if not stale:
                record.status = "DONE"
                record.worker = worker.worker_id
                record.finished = time.time()
                for row in rows:
                    row["worker"] = worker.worker_id
                    row["task_id"] = task_id
                self.rows.extend(rows)
                self.finished_tasks += 1
                worker.chunks_done += 1
                worker.rows_done += len(rows)
                worker.busy_s += duration
            if worker.inflight == task_id:
                worker.inflight = None
            if worker.alive:
                worker.state = "READY"
            done = self.finished_tasks
            failed = self.failed_tasks
            total = self.total_tasks

        if stale:
            print(f"[master] hasil basi diabaikan: {task_id}")
        else:
            self._event(
                "task_done",
                task=task_id,
                worker=worker.worker_id,
                attempt=record.attempt,
                duration_s=round(duration, 3),
                rows=len(rows),
            )
            print(
                f"[master] {done + failed}/{total} task selesai "
                f"({len(rows)} target, {duration * 1000:.0f} ms) "
                f"dari {worker.worker_id}"
            )
        self._assign(worker)

    def _requeue(self, worker: WorkerInfo | None, record: TaskRecord, reason: str) -> None:
        event = None
        failed = False
        with self.lock:
            if record.status != "RUNNING":
                return
            record.worker = None
            failed = record.attempt > self.args.max_retries
            if failed:
                record.status = "FAILED"
                self.failed_tasks += 1
                self.failures.append(
                    {
                        "task_id": record.task_id,
                        "reason": reason,
                        "attempt": record.attempt,
                    }
                )
                print(
                    f"[master] task {record.task_id} GAGAL setelah "
                    f"{record.attempt} percobaan ({reason})"
                )
            else:
                record.status = "QUEUED"
                record.priority = 0  # dahulukan task yang pernah gagal/timeout
                self.tasks.put((0, next(self.seq), record.task_id))
                print(
                    f"[master] task {record.task_id} dikembalikan ke antrean "
                    f"(percobaan {record.attempt}, alasan: {reason})"
                )
            if worker is not None:
                if worker.inflight == record.task_id:
                    worker.inflight = None
                if worker.alive:
                    worker.state = "READY"
            event = {
                "task": record.task_id,
                "worker": worker.worker_id if worker else None,
                "attempt": record.attempt,
                "reason": reason,
            }
        self._event("task_failed" if failed else "task_requeued", **event)

    def _worker_gone(self, worker: WorkerInfo, reason: str) -> None:
        lost_event = None
        requeue_event = None
        requeue_failed = False
        with self.lock:
            if not worker.alive:
                return
            worker.alive = False
            record = self.ledger.get(worker.inflight) if worker.inflight else None
            inflight = record.task_id if record is not None else None
            if record is not None and record.status == "RUNNING":
                record.worker = None
                requeue_failed = record.attempt > self.args.max_retries
                if requeue_failed:
                    record.status = "FAILED"
                    self.failed_tasks += 1
                    self.failures.append(
                        {
                            "task_id": record.task_id,
                            "reason": reason,
                            "attempt": record.attempt,
                        }
                    )
                else:
                    record.status = "QUEUED"
                    record.priority = 0
                    self.tasks.put((0, next(self.seq), record.task_id))
                    print(
                        f"[master] task {record.task_id} dikembalikan ke antrean "
                        f"karena worker keluar ({reason})"
                    )
                requeue_event = {
                    "task": record.task_id,
                    "worker": worker.worker_id,
                    "attempt": record.attempt,
                    "reason": f"worker keluar: {reason}",
                }
            worker.inflight = None
            if self.workers.get(worker.worker_id) is worker:
                del self.workers[worker.worker_id]
            lost_event = {
                "worker": worker.worker_id,
                "reason": reason,
                "inflight": inflight,
            }
            remaining = len(self.workers)

        self._event("worker_lost", **lost_event)
        expected = self.args.expected_workers
        ui.notify(
            "NODE-",
            f"{lost_event['worker']} keluar: {reason} | "
            f"sisa {remaining}/{expected or '?'}",
        )
        if requeue_event is not None:
            self._event(
                "task_failed" if requeue_failed else "task_requeued", **requeue_event
            )
        try:
            worker.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            worker.sock.close()
        except OSError:
            pass

    def _maybe_finish(self) -> bool:
        with self.lock:
            if self.done_since is not None:
                if time.time() - self.done_since >= self.args.grace:
                    self.stop.set()
                    return True
                return False
            complete = self.finished_tasks + self.failed_tasks
            inflight = any(w.inflight for w in self.workers.values())
            required = self.required_workers
            enough = (
                len(self.seen_workers) >= required
                if required > 0
                else bool(self.seen_workers)
            )
            if self.total_tasks > 0 and complete >= self.total_tasks and not inflight and enough:
                self.done_since = time.time()
        return False

    def _send(self, worker: WorkerInfo, msg: dict) -> bool:
        try:
            common.send_msg(worker.sock, msg, worker.send_lock)
            return True
        except (OSError, ValueError):
            return False

    def _status_loop(self) -> None:
        """Cetak daftar node yang terhubung secara berkala."""
        interval = float(self.args.status_interval)
        if interval <= 0:
            return
        while not self.stop.wait(interval):
            with self.lock:
                workers = sorted(self.workers.values(), key=lambda item: item.worker_id)
            expected = self.required_workers or self.args.expected_workers
            total = f"{len(workers)}/{expected}" if expected else str(len(workers))
            if not workers:
                ui.notify("NODES", f"{total} aktif - menunggu node terhubung ...")
                continue
            items = [
                f"{worker.worker_id}@{worker.ip or '?'}:"
                f"{'sibuk' if worker.inflight else 'idle'}"
                for worker in workers
            ]
            ui.notify("NODES", f"{total} aktif | " + " ".join(items))

    # ------------------------------------------------------------------
    # Penutupan & laporan
    # ------------------------------------------------------------------

    def _shutdown_workers(self) -> None:
        with self.lock:
            workers = list(self.workers.values())
        for worker in workers:
            self._send(worker, {"type": "BYE", "reason": "pekerjaan selesai"})
        if workers:
            time.sleep(0.2)
        for worker in workers:
            try:
                worker.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                worker.sock.close()
            except OSError:
                pass

    def finalize(self) -> None:
        duration = time.time() - self.start_time
        with self.lock:
            rows = list(self.rows)
            failures = list(self.failures)
            worker_stats = []
            for worker in self.worker_history.values():
                busy = worker.busy_s
                worker_stats.append(
                    {
                        "worker_id": worker.worker_id,
                        "hostname": worker.hostname,
                        "ip": worker.ip,
                        "health_port": worker.health_port,
                        "cores": worker.cores,
                        "chunks_done": worker.chunks_done,
                        "rows_done": worker.rows_done,
                        "busy_s": round(busy, 3),
                        "connected_s": round(time.time() - worker.connected_at, 3),
                    }
                )
            workers_seen = sorted(self.seen_workers)
            done = self.finished_tasks
            failed = self.failed_tasks
            total = self.total_tasks

        busy_values = [w["busy_s"] for w in worker_stats if w["busy_s"] > 0]
        imbalance = None
        if len(busy_values) > 1 and sum(busy_values) > 0:
            average = sum(busy_values) / len(busy_values)
            imbalance = round(max(busy_values) / average, 3)

        row_summary = common.summarize_rows(rows)
        throughput = self.target_total / duration if duration > 0 else 0.0

        summary = {
            "run_tag": self.args.run_tag,
            "mode": self.args.mode,
            "bind": f"{self.args.bind}:{self.args.port}",
            "probe": {
                "kind": self.args.probe_kind,
                "port": self.args.probe_port,
                "count": self.args.count,
                "timeout": self.args.timeout,
            },
            "workers_expected": self.args.expected_workers,
            "workers_required": self.required_workers,
            "target_source": self.target_source_used,
            "fairness": getattr(self.args, "fairness", "count"),
            "workers_seen": workers_seen,
            "worker_stats": worker_stats,
            "tasks_total": total,
            "tasks_done": done,
            "tasks_failed": failed,
            "targets_total": self.target_total,
            "duration_s": round(duration, 3),
            "throughput_targets_s": round(throughput, 2),
            "imbalance_ratio": imbalance,
            "rows": len(rows),
            "up": row_summary.get("up"),
            "down": row_summary.get("down"),
            "avg_rtt_ms": row_summary.get("avg_rtt"),
            "avg_loss_pct": row_summary.get("avg_loss_pct"),
            "failures": failures,
            "started_at": self.start_time,
            "finished_at": time.time(),
        }

        out_dir = Path(self.args.out)
        out_dir.mkdir(parents=True, exist_ok=True)
        tag = self.args.run_tag
        rows_path = out_dir / f"rows_{tag}.csv"
        summary_path = out_dir / f"ringkasan_{tag}.json"
        common.write_rows_csv(rows_path, rows)
        summary_path.write_text(
            json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        self._event(
            "finish",
            tasks_done=done,
            tasks_failed=failed,
            targets_total=self.target_total,
            duration_s=round(duration, 3),
            imbalance=imbalance,
        )

        print("\n" + "=" * 64)
        print(f"[master] SELESAI  tag={tag}  mode={self.args.mode}")
        print(f"  durasi        : {duration:.2f} s")
        print(f"  fairness      : {getattr(self.args, 'fairness', 'count')}")
        print(f"  target        : {self.target_total} ({row_summary.get('up')} up / {row_summary.get('down')} down)")
        print(f"  sumber target : {self.target_source_used or '-'}")
        print(f"  task          : {done} selesai, {failed} gagal dari {total}")
        print(f"  throughput    : {throughput:.2f} target/detik")
        print(f"  worker        : {len(workers_seen)} terdaftar")
        if imbalance is not None:
            print(f"  imbalance     : {imbalance:.2f} (max/mean waktu sibuk)")
        print(f"  hasil         : {rows_path}")
        print(f"  ringkasan     : {summary_path}")
        print("=" * 64)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Master Distributed Network Monitoring (TCP length-prefixed JSON)"
    )
    parser.add_argument(
        "--bind",
        default=common.interface_ip() or "0.0.0.0",
        help="alamat bind (default: IP wlp3s0/LAN, fallback 0.0.0.0)",
    )
    parser.add_argument("--port", type=int, default=common.CONTROL_PORT)
    parser.add_argument(
        "--targets",
        default=None,
        help="file daftar target (default: tidak dipakai; target = IP worker)",
    )
    parser.add_argument(
        "--target-source",
        choices=["workers", "file", "both"],
        default=None,
        help="sumber target: ip worker terhubung, file, atau keduanya (auto)",
    )
    parser.add_argument("--synth", type=int, default=0, help="buat N target sintetis")
    parser.add_argument("--seed", type=int, default=70)
    parser.add_argument("--repeat", type=int, default=1, help="ulangi daftar target")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument(
        "--chunk",
        type=int,
        default=0,
        help="target per task (0=auto: 1 untuk IP worker, 50 untuk file/sintetis)",
    )
    parser.add_argument(
        "--discovery-timeout",
        type=float,
        default=30.0,
        help="detik menunggu worker sebelum target IP disusun",
    )
    parser.add_argument("--mode", choices=["dynamic", "static"], default="dynamic")
    parser.add_argument("--expected-workers", type=int, default=0)
    parser.add_argument("--probe-kind", choices=["tcp", "icmp"], default="tcp")
    parser.add_argument("--probe-port", type=int, default=80)
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=0.8)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument(
        "--fairness",
        choices=["count", "idle", "off"],
        default="count",
        help="kebijakan pembagian task: count=merata, idle=work-conserving, off=bebas",
    )
    parser.add_argument("--hb-timeout", type=float, default=6.0)
    parser.add_argument("--slack", type=float, default=8.0)
    parser.add_argument("--task-timeout-cap", type=float, default=600.0)
    parser.add_argument("--max-runtime", type=float, default=0.0, help="0 = tanpa batas")
    parser.add_argument("--grace", type=float, default=2.0, help="jeda sebelum master keluar")
    parser.add_argument("--out", default="hasil")
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="tampilkan form parameter di terminal (auto jika tanpa argumen)",
    )
    parser.add_argument(
        "--status-interval",
        type=float,
        default=5.0,
        help="interval cetak daftar node terhubung (0 = mati)",
    )
    parser.add_argument(
        "--events",
        default="",
        help="tulis event JSONL ke file ini (untuk analisis fault tolerance)",
    )
    parser.add_argument("--run-tag", default="dinamis")
    args = parser.parse_args(argv)

    if args.target_source is None:
        args.target_source = "file" if args.targets else "workers"
    if args.target_source in ("file", "both") and not args.targets:
        args.targets = "targets.txt"
    return args


def run_interactive(args):
    fields = [
        ui.Field("mode", "Mode penjadwalan", args.mode, "choice", ("dynamic", "static")),
        ui.Field("port", "Port TCP master", args.port, "int", minimum=1),
        ui.Field(
            "target_source",
            "Sumber target",
            args.target_source,
            "choice",
            ("workers", "file", "both"),
        ),
        ui.Field("targets", "File target (file/both)", args.targets or ""),
        ui.Field("synth", "Target sintetis (0=tidak)", args.synth, "int", minimum=0),
        ui.Field("expected_workers", "Jumlah worker", args.expected_workers, "int", minimum=0),
        ui.Field(
            "discovery_timeout",
            "Tunggu node (detik)",
            args.discovery_timeout,
            "float",
            minimum=0,
        ),
        ui.Field("chunk", "Target/task (0=auto)", args.chunk, "int", minimum=0),
        ui.Field(
            "fairness",
            "Kebijakan fairness",
            args.fairness,
            "choice",
            ("count", "idle", "off"),
        ),
        ui.Field("probe_kind", "Jenis probe", args.probe_kind, "choice", ("tcp", "icmp")),
        ui.Field("probe_port", "Port probe TCP", args.probe_port, "int", minimum=1),
        ui.Field("count", "Probe per target", args.count, "int", minimum=1),
        ui.Field("timeout", "Timeout (detik)", args.timeout, "float", minimum=0.05),
        ui.Field("run_tag", "Nama run", args.run_tag),
    ]
    values = ui.run_form(
        "MASTER - Distributed Network Monitoring",
        fields,
        "Tekan nomor untuk mengubah, Enter untuk mulai",
    )
    for key, value in values.items():
        setattr(args, key, value)
    return args


def main(argv=None) -> int:
    raw_args = sys.argv[1:] if argv is None else list(argv)
    args = parse_args(argv)
    if ui.should_prompt(args.interactive, raw_args):
        args = run_interactive(args)

    if args.targets and args.target_source == "workers" and not args.synth:
        ui.notify("MASTER", "--targets diabaikan karena target-source=workers")

    if args.synth:
        source = f"sintetis:{args.synth}"
    elif args.target_source == "both":
        source = f"file:{args.targets}+ip-worker"
    elif args.target_source == "file":
        source = f"file:{args.targets}"
    else:
        source = "ip-worker"

    ui.notify(
        "MASTER",
        f"mode={args.mode} port={args.port} target={source} "
        f"worker={args.expected_workers or '-'} chunk={args.chunk or 'auto'} "
        f"fairness={args.fairness} "
        f"probe={args.probe_kind}:{args.probe_port}x{args.count} "
        f"timeout={args.timeout}s tag={args.run_tag}",
    )

    master = Master(args)
    if not master.defers_targets():
        if not master.build_tasks():
            return 2
    master.serve()
    return 0 if master.failed_tasks == 0 and master.targets_built else 2


if __name__ == "__main__":
    raise SystemExit(main())
