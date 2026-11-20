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
├── common.py      # framing TCP, probe TCP, util data
├── master.py      # server 9998, scheduler, ledger, agregasi
├── worker.py      # client, heartbeat, ProcessPoolExecutor
├── baseline.py    # sequential (T1) dan local multiprocessing
├── benchmark.py   # orkestrasi uji lokal + laporan
├── faulttest.py   # uji fault tolerance otomatis (kill worker + recovery time)
├── hasil/         # rows_*.csv, ringkasan_*.json
└── laporan/       # tabel_hasil.md, grafik, log
```

Tidak ada dependensi wajib (hanya pustaka standar Python 3.10+).
`matplotlib` opsional (untuk grafik laporan):

```bash
pip install matplotlib
```

---

## 2.1 Sumber target: IP worker atau sintetis

Master punya dua mode sumber target:

| Mode | Arti | Kapan dipakai |
| --- | --- | --- |
| `workers` (default) | Target = IP LAN semua worker yang sedang terhubung | Uji terdistribusi: node saling memantau |
| `sintetis` (`--synth N`) | Target buatan acak | Uji beban dan timeout lokal |

### Alamat & antarmuka LAN

Baik master maupun worker memakai IP antarmuka **wlp3s0** (bisa diganti lewat
`NETMON_IFACE`, mis. `wlan0` di Termux atau `eth0` di VM):

- Master otomatis bind ke IP wlp3s0. Jika antarmuka belum siap, master
  fallback ke `0.0.0.0` agar tetap start.
- Worker membuka health listener di IP wlp3s0 dan melaporkan IP itu di pesan
  `HELLO`. Master memakai IP laporan tersebut sebagai target probe.
- Worker **tanpa argumen** langsung menyambung ke master default
  `10.199.101.109` (ubah dengan `NETMON_MASTER` atau `--master`). Tidak ada
  form otomatis di sisi worker.

Dengan mode `workers`, **tidak perlu scan subnet**. Worker yang aktif akan
connect ke master, master mencatat `IP:health_port`-nya, lalu membagikan probe
ke seluruh worker.

```bash
# master
python3 master.py --expected-workers 9 \
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
Target worker selalu memakai health port masing-masing.

Catatan: task dibagikan ke worker yang sedang idle, jadi satu target belum
tentu diprobe oleh semua node (bukan matriks penuh N×N). `--repeat R` membuat
setiap target disampel R kali, dan hasilnya mencatat `worker` pelaksana di CSV.

Jika `--expected-workers` tidak diisi, master menunggu 2 detik setelah node
pertama untuk mengumpulkan node yang menyusul. Untuk hasil pasti, tetap isi
`--expected-workers` sesuai jumlah node.

> Master memakai IP LAN yang dilaporkan worker (`wlp3s0`). Karena itu mode ini
> cocok untuk node di LAN/lab yang saling terjangkau.

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
python3 master.py --expected-workers 2 --repeat 3 \
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
  [1] Target sintetis (0=worker) : 0
  [2] Jumlah worker              : 0
  [3] Nama run                   : dinamis
  [0] Mulai
```

Ketik nomor untuk mengubah nilai, Enter untuk mulai. Parameter lain memakai
default yang aman (port 9998, mode `dynamic`, probe TCP, dan seterusnya) dan
tetap bisa diubah lewat argumen CLI bila diperlukan.

Form worker (hanya jika `--interactive`): alamat master, port, ID, jumlah core,
**port health**, interval heartbeat, dan sambung ulang (y/n).

Form otomatis dilewati jika stdin bukan TTY (mis. dijalankan lewat pipe atau
penjadwal otomatis). Untuk mencoba form tanpa menunggu worker, pakai target
sintetis:

```bash
python3 master.py --interactive --synth 200
```

Notifikasi yang dicetak:

```
[19:28:45] [MASTER] mode=dynamic port=9998 target=ip-worker ...
[19:28:45] [master] MASTER device=lab-master ip=10.199.101.109:9998
[19:28:45] [master] mendengarkan TCP 10.199.101.109:9998 (harap buka port ini di firewall)
[19:28:45] [MASTER] menunggu node terhubung (butuh 4, maks 30 detik) ...
[19:28:45] [NODE+] L1 device=lab-pc1 ip=10.199.101.12 health=9997 (2 core) dari 10.199.101.12:50216 | total 1/4
[19:28:46] [NODES] 2/4 aktif | master=lab-master@10.199.101.109:9998 | L1@lab-pc1/10.199.101.12:sibuk L2@lab-pc2/10.199.101.13:idle
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
python3 master.py --expected-workers 9 \
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

Master otomatis keluar setelah semua task selesai (jeda `--grace` 2 detik),
mengirim `BYE`, lalu menulis laporan.

> Mode `workers` menunggu node maksimal `--discovery-timeout` detik. Setelah itu
> master memakai node yang ada; `--expected-workers` juga memengaruhi syarat
> selesai. Pada mode sintetis, master menunggu sampai `--expected-workers`
> terpenuhi atau `--max-runtime` habis (0 = tanpa batas).

---

## 5. Matriks pengujian

| Skenario | Perintah / perlakuan |
| -------- | -------------------- |
| a. Sequential | `baseline.py --mode sequential --synth 500 --run-tag sequential` |
| b. Local parallel | `baseline.py --mode local --cores N --run-tag local_par` |
| c. Distributed | `master.py --expected-workers K --repeat N` + K worker |
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
python3 master.py --events laporan/events.jsonl ...
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
- **Target blackhole**: `192.0.2.0/24` (TEST-NET-1, RFC 5737) dipakai untuk
  memunculkan timeout deterministik pada uji lokal.

---

## 9. Etika

Pemantauan hanya dilakukan terhadap host milik sendiri atau laboratorium yang
memberi izin. Target default adalah **node worker yang memang dijalankan untuk
sistem ini**, sehingga pemilik node sudah menyetujui pemantauan. Jangan
mengarahkan probe ke IP publik pihak lain.
