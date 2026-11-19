"""Utilitas bersama untuk Distributed Network Monitoring (Tema 13).

Berisi:
- framing TCP length-prefixed JSON,
- fungsi probe TCP connect,
- generator target sintetis,
- penulisan CSV.
"""

from __future__ import annotations

import csv
import json
import os
import random
import re
import socket
import struct
import subprocess
import time
from pathlib import Path
from statistics import mean, pstdev

# ---------------------------------------------------------------------------
# Konstanta protokol
# ---------------------------------------------------------------------------

CONTROL_PORT = 9998
HEALTH_PORT = 9997  # port kecil di worker agar bisa diprobe dengan TCP
MAX_MSG_BYTES = 4 * 1024 * 1024
PROTO_VERSION = 1

DEFAULT_PROBE = {"kind": "tcp", "port": 80, "count": 3, "timeout": 0.8}

# Antarmuka LAN yang dipakai untuk alamat master/worker. Bisa dioverride lewat
# NETMON_IFACE (mis. wlan0 di Termux atau eth0 di VM).
LAN_INTERFACE = os.environ.get("NETMON_IFACE", "wlp3s0")

# Alamat master untuk mode "langsung jalan" tanpa argumen. Jika IP master
# berubah, cukup set NETMON_MASTER atau jalankan worker dengan --master.
DEFAULT_MASTER = os.environ.get("NETMON_MASTER", "10.199.101.109")


def interface_ip(interface: str | None = None) -> str:
    """Kembalikan IPv4 pada antarmuka LAN (default wlp3s0).

    Urutan: baca `ip -4 addr` untuk antarmuka yang diminta, lalu fallback ke
    alamat keluar default (berguna di Termux/VM yang tidak punya perintah `ip`).
    String kosong berarti tidak ada alamat yang bisa dipakai.
    """
    iface = (interface or LAN_INTERFACE).strip()
    if iface:
        try:
            result = subprocess.run(
                ["ip", "-4", "-o", "addr", "show", "dev", iface],
                capture_output=True,
                text=True,
                timeout=2,
            )
            match = re.search(r"\binet\s+(\d+\.\d+\.\d+\.\d+)", result.stdout)
            if match:
                return match.group(1)
        except (OSError, subprocess.SubprocessError):
            pass

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            # UDP connect tidak mengirim paket; hanya memilih rute keluar.
            probe.connect(("8.8.8.8", 80))
            ip = probe.getsockname()[0]
            if ip and not ip.startswith("127."):
                return ip
    except OSError:
        pass
    return ""


def default_master_ip() -> str:
    """Alamat master default agar `python worker.py` langsung bisa jalan."""
    return os.environ.get("NETMON_MASTER") or DEFAULT_MASTER


ROW_FIELDS = [
    "ip",
    "status",
    "port_open",
    "sent",
    "received",
    "loss_pct",
    "rtt_min",
    "rtt_avg",
    "rtt_max",
    "jitter",
    "probe",
    "ts",
    "worker",
    "task_id",
]

# ---------------------------------------------------------------------------
# Framing TCP: 4 byte panjang (big-endian) + JSON UTF-8
# ---------------------------------------------------------------------------


def send_msg(sock: socket.socket, obj: dict, lock=None) -> None:
    """Kirim satu pesan JSON dengan awalan panjang 4 byte."""
    payload = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    if len(payload) > MAX_MSG_BYTES:
        raise ValueError(f"pesan terlalu besar: {len(payload)} byte")
    frame = struct.pack("!I", len(payload)) + payload
    if lock is None:
        sock.sendall(frame)
    else:
        with lock:
            sock.sendall(frame)


def recv_exact(sock: socket.socket, size: int):
    """Baca tepat `size` byte; kembalikan None jika koneksi ditutup."""
    buf = bytearray()
    while len(buf) < size:
        chunk = sock.recv(size - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def recv_msg(sock: socket.socket):
    """Baca satu pesan JSON; kembalikan None jika koneksi ditutup."""
    header = recv_exact(sock, 4)
    if header is None:
        return None
    (size,) = struct.unpack("!I", header)
    if size == 0 or size > MAX_MSG_BYTES:
        raise ValueError(f"ukuran pesan tidak valid: {size}")
    body = recv_exact(sock, size)
    if body is None:
        return None
    return json.loads(body.decode("utf-8"))


def set_keepalive(sock: socket.socket) -> None:
    """Aktifkan TCP keepalive agar koneksi mati terdeteksi cepat."""
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    except OSError:
        return
    options = [
        ("TCP_KEEPIDLE", 5),
        ("TCP_KEEPINTVL", 2),
        ("TCP_KEEPCNT", 3),
        ("TCP_USER_TIMEOUT", 8000),
    ]
    for name, value in options:
        opt = getattr(socket, name, None)
        if opt is None:
            continue
        try:
            sock.setsockopt(socket.IPPROTO_TCP, opt, value)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Probe
# ---------------------------------------------------------------------------


def parse_target(entry: str):
    """Pisahkan target menjadi (host, port).

    Target boleh berupa `ip` saja atau `ip:port` (mis. target dari worker).
    Port None berarti pakai port default dari probe.
    """
    text = (entry or "").strip()
    if text.count(":") == 1:
        host, _, port_text = text.partition(":")
        if host and port_text.isdigit():
            return host, int(port_text)
    return text, None


def _tcp_attempt(ip: str, port: int, timeout: float):
    """Satu percobaan TCP connect.

    Hasil: (status, rtt_ms) dengan status open/closed/timeout/unreachable.
    'closed' berarti host hidup tetapi port menolak (RST).
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    started = time.perf_counter()
    try:
        sock.connect((ip, port))
        return "open", (time.perf_counter() - started) * 1000.0
    except ConnectionRefusedError:
        return "closed", (time.perf_counter() - started) * 1000.0
    except (socket.timeout, TimeoutError):
        return "timeout", None
    except OSError:
        return "unreachable", None
    finally:
        try:
            sock.close()
        except OSError:
            pass


def _probe_tcp_target(target: str, probe: dict) -> dict:
    count = max(1, int(probe.get("count", 3)))
    host, target_port = parse_target(target)
    port = int(target_port or probe.get("port", 80))
    timeout = float(probe.get("timeout", 0.8))

    samples = []
    responses = 0
    port_open = False

    for _ in range(count):
        status, rtt = _tcp_attempt(host, port, timeout)
        if status == "open":
            port_open = True
        if rtt is not None:
            samples.append(rtt)
            responses += 1

    sent = count
    loss = (sent - responses) / sent * 100.0 if sent else 0.0

    return {
        "ip": host,
        "status": "up" if responses else "down",
        "port_open": port_open,
        "sent": sent,
        "received": responses,
        "loss_pct": round(loss, 2),
        "rtt_min": round(min(samples), 3) if samples else None,
        "rtt_avg": round(mean(samples), 3) if samples else None,
        "rtt_max": round(max(samples), 3) if samples else None,
        "jitter": round(pstdev(samples), 3) if len(samples) > 1 else 0.0,
        "probe": f"tcp:{port}",
        "ts": round(time.time(), 3),
    }


def _probe_icmp_target(target: str, probe: dict) -> dict:
    ip, _ = parse_target(target)
    count = max(1, int(probe.get("count", 3)))
    timeout = float(probe.get("timeout", 0.8))
    is_windows = platform.system().lower() == "windows"

    if is_windows:
        cmd = ["ping", "-n", str(count), "-w", str(int(timeout * 1000)), ip]
    else:
        cmd = [
            "ping",
            "-c",
            str(count),
            "-W",
            str(max(1, int(round(timeout)))),
            "-i",
            "0.2",
            ip,
        ]

    text = ""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=count * timeout + 10,
            check=False,
        )
        text = (result.stdout or "") + (result.stderr or "")
    except (OSError, subprocess.SubprocessError):
        text = ""

    times = [float(x) for x in re.findall(r"time[=<]\s*([\d.]+)\s*ms", text)]
    loss = None
    match = re.search(r"(\d+(?:\.\d+)?)%\s*(?:packet\s+)?loss", text)
    if match:
        loss = float(match.group(1))

    sent = count
    if loss is not None:
        received = round(sent * (1.0 - loss / 100.0))
    else:
        received = len(times)

    if loss is None:
        loss = (sent - received) / sent * 100.0 if sent else 0.0

    return {
        "ip": ip,
        "status": "up" if received else "down",
        "port_open": None,
        "sent": sent,
        "received": received,
        "loss_pct": round(loss, 2),
        "rtt_min": round(min(times), 3) if times else None,
        "rtt_avg": round(mean(times), 3) if times else None,
        "rtt_max": round(max(times), 3) if times else None,
        "jitter": round(pstdev(times), 3) if len(times) > 1 else 0.0,
        "probe": "icmp",
        "ts": round(time.time(), 3),
    }


def probe_target(target: str, probe: dict) -> dict:
    """Jalankan probe satu target sesuai konfigurasi (ip atau ip:port)."""
    kind = (probe.get("kind") or "tcp").lower()
    if kind == "tcp":
        return _probe_tcp_target(target, probe)
    if kind == "icmp":
        return _probe_icmp_target(target, probe)
    raise ValueError(f"jenis probe tidak dikenal: {kind}")


# ---------------------------------------------------------------------------
# Helper data
# ---------------------------------------------------------------------------


def split_evenly(items, parts):
    """Bagi list menjadi `parts` bagian yang ukurannya hampir sama."""
    parts = max(1, int(parts))
    if not items:
        return []
    size, extra = divmod(len(items), parts)
    out = []
    index = 0
    for k in range(parts):
        take = size + (1 if k < extra else 0)
        if take:
            out.append(items[index : index + take])
        index += take
    return out


def load_targets(path: str, repeat: int = 1, limit: int = 0):
    """Baca daftar IP dari file teks (satu per baris, # = komentar)."""
    raw = []
    file_path = Path(path)
    if file_path.exists():
        for line in file_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            raw.append(line)

    if not raw:
        raise SystemExit(f"tidak ada target di {path}")

    expanded = []
    for _ in range(max(1, repeat)):
        expanded.extend(raw)

    if limit and limit > 0:
        expanded = expanded[:limit]
    return expanded


def synth_targets(count: int, reachable_ratio: float = 0.7, seed: int = 70):
    """Buat target sintetis: campuran loopback dan TEST-NET-1 (blackhole).

    TEST-NET-1 (192.0.2.0/24, RFC 5737) tidak dirutekan ke internet sehingga
    menghasilkan timeout deterministik pada banyak jaringan.
    """
    rng = random.Random(seed)
    out = []
    for i in range(count):
        if rng.random() < reachable_ratio:
            out.append("127.0.0.1")
        else:
            out.append(f"192.0.2.{(i % 254) + 1}")
    return out


def write_rows_csv(path: Path, rows, extra_fields=None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(ROW_FIELDS)
    for field in extra_fields or []:
        if field not in fields:
            fields.append(field)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def summarize_rows(rows) -> dict:
    if not rows:
        return {"targets": 0, "up": 0, "down": 0, "avg_rtt": None}
    rtts = [row["rtt_avg"] for row in rows if row.get("rtt_avg") is not None]
    return {
        "targets": len(rows),
        "up": sum(1 for row in rows if row.get("status") == "up"),
        "down": sum(1 for row in rows if row.get("status") != "up"),
        "avg_rtt": round(mean(rtts), 3) if rtts else None,
        "avg_loss_pct": round(mean([row.get("loss_pct", 0.0) for row in rows]), 2),
    }
