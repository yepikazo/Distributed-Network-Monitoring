# Tema 13 — Distributed Network Monitoring

Implementasi master–worker untuk UAS Komputasi Paralel dan Sistem Terdistribusi.
Sistem memantau konektivitas sejumlah alamat IP: latency, packet loss, dan status
koneksi. Seluruh komunikasi memakai **TCP** (tanpa UDP discovery).

- Protokol: length-prefixed JSON (`4 byte uint32` + JSON UTF-8)
- Port kontrol: **9998/tcp** (hanya dibuka di Master)
- Port health worker: **9997/tcp** (agar node bisa diprobe)
- Model: Master = server, Worker = client
- Target default: **IP worker yang benar-benar terhubung** (bukan scan subnet)
- Penjadwalan: pull model, 1 task in-flight per worker
- Fault tolerance: heartbeat + timeout + requeue + retry + dedup

---

## 1. Pemenuhan komponen UAS

| Komponen | Implementasi |
| -------- | ------------ |
| Task decomposition | Target (default: IP worker terhubung) dipecah menjadi task kecil; worker memecah lagi per core |
| Parallel processing | `ProcessPoolExecutor` di setiap worker |
| Distributed processing | Master–worker lewat TCP, hasil dikumpulkan di master |
| Dynamic load balancing | Worker menarik task saat idle; chunk dinamis; prioritas retry |
| Fault tolerance | Heartbeat 2 s, task timeout, requeue, retry maks 2×, ledger |
| Performance evaluation | `baseline.py`, `benchmark.py`, CSV, tabel, grafik |

---

## 2. Struktur file

```
tema13/
├── common.py      # framing TCP, probe TCP/ICMP, util data
├── master.py      # server 9998, scheduler, ledger, agregasi
├── worker.py      # client, heartbeat, ProcessPoolExecutor
├── baseline.py    # sequential (T1) dan local multiprocessing
├── benchmark.py   # orkestrasi uji lokal + laporan
├── faulttest.py   # uji fault tolerance otomatis (kill worker + recovery time)
├── targets.txt    # opsional: daftar IP untuk target-source=file
├── Dockerfile     # image Python + iputils-ping + psutil
├── docker-compose.yml
├── .env           # jumlah worker, jumlah target, dsb.
├── TERMUX.md      # panduan menjalankan di Android/Termux
├── hasil/         # rows_*.csv, ringkasan_*.json
└── laporan/       # tabel_hasil.md, grafik, log
```

Tidak ada dependensi wajib (hanya pustaka standar Python 3.10+).
`psutil` dan `matplotlib` opsional:

```bash
pip install psutil matplotlib
```

---

## 2.1 Sumber target: IP worker atau file

Master punya tiga mode sumber target:

| Mode | Arti | Kapan dipakai |
| --- | --- | --- |
| `workers` (default) | Target = IP LAN semua worker yang sedang terhubung | Uji terdistribusi: node saling memantau |
| `file` | Target dibaca dari `targets.txt` | Memantau perangkat lain di lab |
| `both` | Gabungan file + IP worker | Demo lengkap |

### Alamat & antarmuka LAN

Baik master maupun worker memakai IP antarmuka **wlp3s0** (bisa diganti lewat
`NETMON_IFACE`, mis. `wlan0` di Termux atau `eth0` di container):

- Master otomatis bind ke IP wlp3s0. Jika antarmuka belum siap, master
  fallback ke `0.0.0.0` agar tetap start.
- Worker membuka health listener di IP wlp3s0 dan melaporkan IP itu di pesan
  `HELLO`. Master memakai IP laporan tersebut sebagai target probe.
- Worker **tanpa argumen** langsung menyambung ke master default
  `10.199.101.109` (ubah dengan `NETMON_MASTER` atau `--master`). Tidak ada
  form otomatis di sisi worker.

Dengan mode `workers`, **tidak perlu scan subnet dan tidak perlu mengisi
`targets.txt`**. Worker yang aktif akan connect ke master, master mencatat
`IP:health_port`-nya, lalu membagikan probe ke seluruh worker.

```bash
# master (tanpa --targets)
python3 master.py --target-source workers --expected-workers 9 \
    --repeat 3 --run-tag ip_worker --out hasil

# worker: langsung jalan, memakai master default 10.199.101.109
python3 worker.py --cores 2
# atau tentukan master lain
python3 worker.py --master 10.199.101.109 --port 9998 --cores 2
```

Worker membuka **health port TCP 9997** (bisa diubah dengan `--health-port`,
`0` = acak). Port ini hanya menerima koneksi lalu menutupnya, dipakai sebagai
sasaran probe TCP. Jika 9997 sudah terpakai (mis. beberapa worker di satu
komputer), worker otomatis memakai port acak dan melaporkannya ke master.
`--probe-port` hanya dipakai untuk target file yang tidak menulis port sendiri;
target worker selalu memakai health port masing-masing.

Dengan `--probe-kind icmp`, target worker memakai IP saja (health port tidak
dipakai), tetapi `ping` harus diizinkan di setiap worker.

Catatan: task dibagikan ke worker yang sedang idle, jadi satu target belum
tentu diprobe oleh semua node (bukan matriks penuh N×N). `--repeat R` membuat
setiap target disampel R kali, dan hasilnya mencatat `worker` pelaksana di CSV.

Jika `--expected-workers` tidak diisi, master menunggu 2 detik setelah node
pertama untuk mengumpulkan node yang menyusul. Untuk hasil pasti, tetap isi
`--expected-workers` sesuai jumlah node.

> Master memakai IP LAN yang dilaporkan worker (`wlp3s0`). Karena itu mode ini
> cocok untuk node di LAN/lab yang saling terjangkau. Untuk host di balik NAT,
> gunakan `--target-source file` dengan IP publik/port yang benar.

Kebijakan pembagian task (`--fairness`):

| Nilai | Perilaku |
| --- | --- |
| `count` (default) | Jumlah task (selesai + in-flight) diseimbangkan antar-node |
| `idle` | Hanya mendahulukan node idle yang tertinggal; worker cepat boleh lanjut |
| `off` | Murni siapa cepat dia dapat (throughput maksimum) |

`benchmark.py` dan `faulttest.py` memakai `idle` agar pengukuran performa tidak
tertahan oleh pemerataan.

---

## 3. Uji cepat (1 komputer)

```bash
cd UAS/tema13

# 1) sequential baseline
python3 baseline.py --synth 60 --count 1 --timeout 0.3 --mode sequential

# 2a) target = IP worker yang terhubung (default)
python3 master.py --target-source workers --expected-workers 2 --repeat 3 \
    --count 1 --timeout 0.3 --run-tag lokal_worker --out hasil

# 2b) target sintetis (untuk uji beban/timeout)
python3 master.py --synth 60 --count 1 --timeout 0.3 --chunk 10 \
    --expected-workers 2 --run-tag lokal --out hasil

# jalankan 2 worker di terminal lain (langsung tanpa opsi)
python3 worker.py --cores 2
python3 worker.py --cores 2
```

Worker default menyambung ke `10.199.101.109:9998`. Jika IP master berbeda,
set `NETMON_MASTER=<IP-master>` atau tambahkan `--master <IP-master>`.

Atau otomatis:

```bash
python3 benchmark.py local --workers 1 2 4 --synth 60 --count 1 --timeout 0.3
python3 benchmark.py report
python3 benchmark.py plan     # panduan 10 node fisik
```

---

## 3.1 Form interaktif & notifikasi

`master.py` yang dijalankan **tanpa argumen di terminal** menampilkan form untuk
mengubah parameter sebelum mulai. Worker tidak menampilkan form otomatis: tanpa
argumen worker langsung menyambung ke master default. Form worker tetap bisa
dibuka secara eksplisit dengan `--interactive`.

```bash
python3 master.py --interactive
python3 worker.py                  # langsung jalan
python3 worker.py --interactive    # form opsional
```

Form master:

```
  [1] Mode penjadwalan          : dynamic
  [2] Port TCP master           : 9998
  [3] Sumber target             : workers
  [4] File target (file/both)   : (kosong)
  [5] Target sintetis (0=tidak) : 0
  [6] Jumlah worker             : 0
  [7] Tunggu node (detik)       : 30
  [8] Target/task (0=auto)      : 0
  [9] Kebijakan fairness        : count
  [10] Jenis probe              : tcp
  [11] Port probe TCP           : 80
  [12] Probe per target         : 3
  [13] Timeout (detik)          : 0.8
  [14] Nama run                 : dinamis
  [0] Mulai
```

Ketik nomor untuk mengubah nilai, Enter untuk mulai. Validasi langsung
(mis. `mode` hanya `dynamic`/`static`, `timeout` minimal 0.05).

Form worker (hanya jika `--interactive`): alamat master, port, ID, jumlah core,
**port health**, interval heartbeat, dan sambung ulang (y/n).

Di Docker, form otomatis dilewati karena stdin bukan TTY. Untuk mencoba form
tanpa menjalankan worker, pakai target sintetis:

```bash
docker compose run --rm -it master python3 master.py --interactive --synth 200
```

Notifikasi yang dicetak:

```
[19:28:45] [MASTER] mode=dynamic port=9998 target=ip-worker ...
[19:28:45] [master] mendengarkan TCP 10.199.101.109:9998 (harap buka port ini di firewall)
[19:28:45] [MASTER] menunggu node terhubung (butuh 4, maks 30 detik) ...
[19:28:45] [NODE+] L1 (lab-pc1, 2 core) IP 10.199.101.12 dari 10.199.101.12:50216 | total 1/4
[19:28:46] [NODES] 2/4 aktif | L1@10.199.101.12:sibuk L2@10.199.101.13:idle
[19:28:52] [NODE-] L2 keluar: koneksi ditutup | sisa 1/4
```

Di sisi worker:

```
[19:28:45] [WORKER] health port TCP 9997 di 10.199.101.12 siap diprobe
[19:28:45] [WORKER] menyambung ke 10.199.101.109:9998 ...
[19:28:45] [WORKER] TERHUBUNG ke master 10.199.101.109:9998 sebagai L1 (2 core, IP 10.199.101.12), chunk=auto, health=9997
[19:28:45] [TASK] T0002-e11bd1: 1 target (1 up), 210 ms
```

Daftar node dicetak tiap 5 detik. Atur dengan `--status-interval` (0 = mati):

```bash
python3 master.py --status-interval 2
```

---

## 4. Menjalankan di 10 node fisik

**Master** (mis. `10.199.101.109`):

```bash
# buka port kontrol
sudo ufw allow 9998/tcp
# opsional: batasi ke subnet lab
# sudo ufw allow from 10.199.101.0/24 to any port 9998 proto tcp

# target = IP 9 worker yang terhubung, tiap node diprobe 5 kali
python3 master.py --target-source workers --expected-workers 9 \
    --repeat 5 --count 1 --timeout 0.5 --discovery-timeout 30 \
    --mode dynamic --run-tag dist_ipworker_k9 --out hasil
```

**Setiap worker** (9 mesin):

```bash
# izinkan node lain memprobe health port worker ini
sudo ufw allow 9997/tcp

# langsung jalan: master default 10.199.101.109, health bind ke wlp3s0
python3 worker.py --cores 4
```

Worker memakai IP **wlp3s0** masing-masing untuk health listener dan melaporkan
IP itu ke master. Master juga bind ke wlp3s0, jadi pastikan semua node berada di
subnet yang sama (mis. `10.199.101.0/24`).
Master menunggu sampai `--expected-workers` terpenuhi atau
`--discovery-timeout` habis, lalu menyusun target dari `IP:health_port` yang
dilaporkan setiap worker. Jika hanya 7 dari 9 node datang, master tetap lanjut
dengan 7 target (tidak menggantung).

Alternatif memantau perangkat lain di lab (bukan node worker):

```bash
python3 master.py --target-source file --targets targets.txt \
    --mode dynamic --chunk 50 --expected-workers 9 \
    --run-tag dist_file_k9 --out hasil
```

Master otomatis keluar setelah semua task selesai (jeda `--grace` 2 detik),
mengirim `BYE`, lalu menulis laporan.

> Mode `workers` menunggu node maksimal `--discovery-timeout` detik. Setelah itu
> master memakai node yang ada; `--expected-workers` juga memengaruhi syarat
> selesai. Pada mode `file`/`sintetis`, master menunggu sampai
> `--expected-workers` terpenuhi atau `--max-runtime` habis (0 = tanpa batas).

---

## 4.1 Menjalankan di Docker (1 node)

Satu komputer menjalankan 1 container master + N container worker pada network
Compose yang sama. Worker menghubungi master lewat nama service `master`.

Secara default master memantau **IP container worker** (port health 9997 di
network internal Compose), jadi tidak perlu `targets.txt` dan tidak perlu scan
subnet. Container master dan worker saling menjangkau lewat network Compose.

Prasyarat:

```bash
docker --version           # Docker 24+
docker compose version     # Compose v2
```

File yang dipakai: `Dockerfile`, `docker-compose.yml`, `.env`.

### A. Cara CLI

```bash
cd UAS/tema13
mkdir -p hasil laporan

# 1) build image sekali saja
docker build -t netmon-tema13:latest .

# 2) jalankan 1 master + 4 worker sesuai .env
docker compose up

# 3) di terminal lain, pantau log master
docker compose logs -f master
```

Master keluar sendiri setelah semua task selesai, lalu mengirim `BYE` ke worker.
Hasil tersedia langsung di host:

```
hasil/rows_<tag>.csv
hasil/ringkasan_<tag>.json
laporan/events_<tag>.jsonl
```

Hentikan dan bersihkan container:

```bash
docker compose down          # file hasil tidak terhapus
```

Mengubah skenario cukup lewat `.env`:

```dotenv
WORKERS=4            # jumlah worker = --expected-workers
TARGET_SOURCE=workers # workers | file | both
SYNTH=0              # >0 = pakai target sintetis (mengabaikan sumber di atas)
REPEAT=3             # ulangi daftar target (mis. 4 worker x 3 = 12 task)
COUNT=1              # probe per target
TIMEOUT=0.3
CHUNK=0              # 0 = auto (1 untuk IP worker, 50 untuk file/sintetis)
FAIRNESS=count       # count | idle | off
MODE=dynamic         # dynamic | static
WORKER_CORES=1
DISCOVERY_TIMEOUT=30 # detik menunggu worker sebelum target disusun
MAX_RUNTIME=300
TAG=docker           # nama run -> ringkasan_docker.json
```

Contoh variasi:

```bash
# target sintetis seperti versi lama (200 target)
SYNTH=200 REPEAT=1 docker compose up

# mode statis (chunk besar) untuk pembanding load balancing
MODE=static TAG=docker_statis docker compose up

# 6 worker (6 target IP worker)
WORKERS=6 docker compose up
```

> Catatan: `WORKERS` harus sama dengan jumlah container worker. Jika memakai
> `docker compose up --scale worker=6`, set juga `WORKERS=6`.

### B. Baseline dan fault tolerance di dalam container

```bash
# sequential (T1)
docker compose run --rm master python3 baseline.py \
    --synth 200 --count 1 --timeout 0.3 \
    --mode sequential --run-tag docker_seq --out /app/hasil

# fault tolerance: 3 worker, bunuh L1 lalu L2
docker compose run --rm master python3 faulttest.py \
    --workers 3 --kill L1,L2 --stagger 1 \
    --synth 120 --count 1 --timeout 0.2 --chunk 5 \
    --tag docker_fault --out /app/hasil
```

Contoh keluaran fault tolerance yang sudah diuji di container:

```
worker dibunuh : L1, L2
deteksi        : rata-rata 0.003 s
task requeued  : 2
dipulihkan     : 2 task, gagal 0
recovery time  : rata-rata 0.965 s (maks 1.362 s)
task total     : 12 selesai / 12 (gagal 0)
```

### C. Cara Portainer

1. Build image sekali dari terminal (Portainer web editor tidak membawa file
   sumber untuk `COPY`):

   ```bash
   docker build -t netmon-tema13:latest .
   ```

2. Buka Portainer → **Stacks** → **Add stack** → nama `netmon-tema13`.
3. Pilih **Web editor**, tempel isi `docker-compose.yml`, lalu **hapus** baris
   `build: .` pada kedua service (image sudah dibangun pada langkah 1).
4. Karena Portainer menyimpan stack di direktorinya sendiri, ganti volume
   relatif menjadi absolut, mis.:

   ```yaml
   volumes:
     - /home/yepikazo/KPT/UAS/tema13/hasil:/app/hasil
     - /home/yepikazo/KPT/UAS/tema13/laporan:/app/laporan
   ```

5. Buka **Environment variables** dan isi `WORKERS`, `TARGET_SOURCE`, `SYNTH`,
   `REPEAT`, `COUNT`, `TIMEOUT`, `CHUNK`, `FAIRNESS`, `MODE`,
   `DISCOVERY_TIMEOUT`, `TAG` (atau edit langsung di YAML).
6. **Deploy the stack**. Lihat log lewat Containers → `netmon-tema13-master-1`
   → Logs.

### D. Worker di luar Docker (menuju 10 node)

Aktifkan port publik di service master:

```yaml
    ports:
      - "9998:9998"
```

Lalu:

```bash
# beri waktu node luar terhubung (default 30 detik)
DISCOVERY_TIMEOUT=180 WORKERS=9 docker compose up master

# di setiap laptop/PC worker:
python3 worker.py --master <IP-komputer-docker> --port 9998 --cores 2
```

Pastikan firewall host mengizinkan TCP 9998 dari subnet lab. Selain itu,
**health port 9997 di setiap worker** harus bisa dijangkau oleh master dan
worker lain (buka `sudo ufw allow 9997/tcp`). Master memakai IP worker yang
terlihat dari koneksi TCP; jika worker berada di balik NAT, gunakan
`TARGET_SOURCE=file`.

### E. Troubleshooting

| Gejala | Penyebab / solusi |
| --- | --- |
| `port is already allocated` | Port 9998 dipakai proses lain; ubah mapping atau `--port` |
| `Permission denied` saat menulis `hasil/` | File dibuat root; jalankan `sudo chown -R "$USER":"$USER" hasil laporan` |
| Master menunggu terus | Mode `file`/`sintetis` dan jumlah worker < `WORKERS`; tunggu `MAX_RUNTIME` |
| `tidak ada worker ... dibatalkan` | Worker belum start dalam `DISCOVERY_TIMEOUT`; perbesar nilainya |
| Semua target `down` | Health port 9997 tertutup firewall, atau worker di balik NAT |
| `address already in use` pada worker | Beberapa worker di satu host; pakai `--health-port 0` |
| Worker langsung berhenti | Master belum siap; jalankan ulang `docker compose up` |
| 9 worker terasa lambat | Host hanya 4 core → oversubscription; valid untuk fungsi, bukan performa |
| Hasil tidak muncul di host | Bind mount salah (terutama di Portainer); gunakan path absolut |

---

## 4.2 Termux (Android)

Android dapat dipakai sebagai worker tambahan (atau master skala kecil). Panduan
lengkap ada di [`TERMUX.md`](TERMUX.md). Ringkasnya:

```bash
pkg install -y python git
cd ~/tema13
termux-wake-lock
python worker.py --cores 2
```

Worker langsung memakai master default `10.199.101.109`. Di Android, antarmuka
biasanya `wlan0`; worker otomatis fallback ke IP keluar jika `wlp3s0` tidak ada.
Untuk memaksa: `NETMON_IFACE=wlan0 python worker.py --cores 2`.

Jika perangkat membatasi `multiprocessing` (mis. `Errno 38`), worker otomatis
beralih ke mode inline. Untuk memaksa:

```bash
python worker.py --master 10.199.101.109 --port 9998 --cores 1 --inline
```

Worker HP membuka health port **9997/tcp** agar bisa diprobe master. Jika
jaringan memblokir port itu, node akan tampil `down`; gunakan
`--health-port 0` (acak) atau jalankan master dengan `--target-source file`
untuk target lain.

Angka performa dari HP tidak dipakai untuk laporan; gunakan laptop/PC.

---

## 5. Matriks pengujian

| Skenario | Perintah / perlakuan |
| -------- | -------------------- |
| a. Sequential | `baseline.py --mode sequential --run-tag sequential` |
| b. Local parallel | `baseline.py --mode local --cores N --run-tag local_par` |
| c. Distributed | `master.py --target-source workers --expected-workers K --repeat N` + K worker |
| d. Load balancing | `--fairness count` vs `idle`/`off`; `--mode static` vs `dynamic` |
| e. Fault tolerance | `python3 faulttest.py --workers 3 --kill L1,L2` (otomatis) |
| f. Scalability | ulangi (c) untuk K = 1, 2, 4, 6, 9 |

Metrik yang dihasilkan:

- `duration_s`, `throughput_targets_s`
- `speedup = T_sequential / T_K`, `efficiency = speedup / K` (dihitung `report`)
- `imbalance_ratio = max(waktu_sibuk) / mean(waktu_sibuk)`
- recovery time: selisih waktu di log master antara worker keluar dan task selesai

---

## 6. Uji fault tolerance otomatis

`faulttest.py` menjalankan master + N worker lokal, membunuh worker **tepat saat
sedang mengerjakan task**, lalu menghitung recovery time dari event master.

```bash
python3 faulttest.py --workers 3 --kill L1,L2 --stagger 2 --synth 200
```

Keluaran di `laporan/`:

- `fault_<tag>_kills.csv` — worker, waktu kill, waktu deteksi, latency deteksi
- `fault_<tag>_tasks.csv` — task yang di-requeue, waktu selesai, recovery time
- `fault_<tag>_ringkas.json` — agregat (mean/max/median recovery, jumlah gagal)
- `fault_<tag>_events.jsonl` — event mentah master (plan/assign/done/requeue/lost)

Contoh hasil pada mesin penulis (3 worker, 2 worker dibunuh):

```
worker dibunuh : L1, L2
deteksi        : rata-rata 0.004 s
task requeued  : 2
dipulihkan     : 2 task, gagal 0
recovery time  : rata-rata 1.478 s (maks 2.016 s)
task total     : 15 selesai / 15 (gagal 0)
```

Untuk laporan, jalankan dengan jumlah target lebih besar dan 1, 2, atau 3 worker
sekaligus, lalu kutip kolom `detection_s` dan `recovery_s` dari CSV.

Master juga bisa menulis event sendiri tanpa `faulttest.py`:

```bash
python3 master.py --target-source workers --events laporan/events.jsonl ...
```

---

## 7. Format keluaran

`hasil/ringkasan_<tag>.json`:

```json
{
  "run_tag": "dist_ipworker_k9",
  "mode": "dynamic",
  "target_source": "ip-worker",
  "workers_required": 9,
  "tasks_total": 45,
  "tasks_done": 45,
  "tasks_failed": 0,
  "targets_total": 45,
  "duration_s": 12.4,
  "throughput_targets_s": 3.6,
  "imbalance_ratio": 1.12,
  "worker_stats": [{"worker_id": "L1", "ip": "10.199.101.12", "health_port": 9997, "chunks_done": 5, "busy_s": 1.1}]
}
```

`hasil/rows_<tag>.csv` menyimpan satu baris per target:

```
ip,status,port_open,sent,received,loss_pct,rtt_min,rtt_avg,rtt_max,jitter,probe,ts,worker,task_id
```

Catatan semantik: `status=up` berarti host merespons (termasuk RST saat port
tertutup); `port_open` menunjukkan apakah TCP handshake berhasil.

---

## 8. Catatan desain

- **Kenapa master jadi server?** Hanya 1 port kontrol (9998) yang perlu dibuka
  dan worker terdeteksi otomatis saat connect; master tidak perlu daftar IP
  worker atau broadcast.
- **Kenapa target = IP worker?** Node yang aktif sudah “mendaftar” lewat
  koneksi TCP, jadi tidak perlu scan subnet. Master memakai IP peer ditambah
  health port yang dilaporkan worker, lalu membagikan probe ke seluruh node.
  Ini memantau konektivitas antar-node tanpa menyentuh perangkat lain.
- **Kenapa worker membuka health port?** Probe TCP butuh port yang menerima
  koneksi. Port 9997 (atau acak) hanya accept lalu close, sehingga latency dan
  status node terukur. Tanpa port ini semua target worker akan `down`.
- **Kenapa chunk kecil?** Target mati memakan waktu sampai timeout, sedangkan
  target hidup selesai dalam milidetik. Chunk kecil + pull membuat worker cepat
  mengambil lebih banyak task (bukti load balancing). Untuk target IP worker,
  chunk otomatis 1 agar setiap node terdistribusi.
- **Kenapa `ProcessPoolExecutor`?** Memenuhi syarat multiprocessing; setiap
  worker memakai seluruh core-nya untuk satu task.
- **ICMP** butuh hak akses root/CAP_NET_RAW di Linux. Default probe adalah TCP
  connect agar portabel. Gunakan `--probe-kind icmp` bila izin tersedia.
- **Target blackhole**: `192.0.2.0/24` (TEST-NET-1, RFC 5737) dipakai untuk
  memunculkan timeout deterministik pada uji lokal.

---

## 9. Etika

Pemantauan hanya dilakukan terhadap host milik sendiri atau laboratorium yang
memberi izin. Target default adalah **node worker yang memang dijalankan untuk
sistem ini**, sehingga pemilik node sudah menyetujui pemantauan. Jangan
mengarahkan probe ke IP publik pihak lain.
