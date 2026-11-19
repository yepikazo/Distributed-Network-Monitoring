FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# iputils-ping dipakai hanya jika memilih --probe-kind icmp
RUN apt-get update \
    && apt-get install -y --no-install-recommends iputils-ping \
    && rm -rf /var/lib/apt/lists/*

COPY common.py master.py worker.py baseline.py benchmark.py faulttest.py ui.py targets.txt ./

# psutil opsional (metrik CPU/memori pada heartbeat)
RUN pip install --no-cache-dir psutil

RUN mkdir -p /app/hasil /app/laporan

# 9998 = kontrol master, 9997 = health worker (diprobe TCP)
EXPOSE 9998 9997

# Demo/lab: berjalan sebagai root agar bind mount ./hasil dan ./laporan
# langsung bisa ditulis. Untuk produksi, buat user non-root dan chown volume.
CMD ["python3", "master.py", "--help"]
