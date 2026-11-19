# Rancangan Sistem — Tema 13: Distributed Network Monitoring

Dokumen ini adalah rancangan teknis (BAB III) untuk proyek UAS Komputasi Paralel dan
Sistem Terdistribusi. Fokus: komunikasi **murni TCP** (tanpa UDP discovery) antara
1 Master dan 9 Worker.

---

## 1. Keputusan Port

Hasil pemindaian port pada perangkat pengembangan:

| Port | Status | Pemilik |
| ---- | ------ | ------- |
| 22   | terpakai | SSH |
| 3306 | terpakai | MySQL/MariaDB |
| 9000 | terpakai | aplikasi lain |
| 36215 / 49374 | terpakai | localhost (sistem) |
| **9998** | **kosong** | dipakai sebagai port kontrol |

- **Port kontrol: 9998/tcp** (satu port di master, multiplex untuk semua pesan).
  Master bind ke IP `wlp3s0` (fallback `0.0.0.0` bila antarmuka belum siap).
- **Port health worker: 9997/tcp** (menerima koneksi probe lalu menutupnya),
  bind ke IP `wlp3s0` worker. Jika bentrok (beberapa worker di satu host),
  worker memakai port acak dan melaporkannya ke master lewat `HELLO`.
- Port 9999 (UDP discovery lama) **tidak dipakai lagi** karena skema baru murni TCP.
- Hanya **Master** yang membuka port kontrol. Worker membuka health port agar
  dapat diprobe TCP; izinkan 9997/tcp di firewall tiap worker.

Jaringan: `wlp3s0` = `10.199.101.109/24`, subnet `10.199.101.0/24`. `docker0`
(172.17.0.1/16) diabaikan.

---

## 2. Topologi

```
            MASTER  10.199.101.x:9998  (listen)
              ▲   ▲   ▲            ▲
              │   │   │            │   worker selalu menghubungi master
        ┌─────┘   │   └─────┐      │
     Worker1   Worker2   Worker3  ... Worker9
     (masing-masing menjalankan ProcessPoolExecutor)
```

**Master sebagai server, Worker sebagai client.** Alasan:

| Aspek | Master-server (dipilih) | Worker-server |
| ----- | ----------------------- | ------------- |
| Port masuk | 1 port di master | 9 port, 9 firewall |
| Discovery | otomatis saat worker connect | butuh daftar IP statis |
| Deteksi worker mati | socket close / timeout | harus polling tiap worker |
| Kelemahan | master harus punya IP tetap | broadcast/sweep rapuh |

Karena master berada di satu mesin dengan IP tetap, skema ini paling sederhana dan
paling tahan terhadap perubahan topologi.

---

## 3. Framing Pesan

TCP adalah byte stream, sehingga batas pesan harus didefinisikan sendiri:
**length-prefixed JSON**.

```
+----------------+---------------------------+
| 4 byte uint32  |  N byte JSON (UTF-8)      |
| big-endian N   |                           |
+----------------+---------------------------+
```

Aturan implementasi:

- `send_msg(sock, obj)`: `json.dumps` → `struct.pack("!I", len)` → kirim semua byte.
- `recv_exact(sock, n)`: loop sampai `n` byte terkumpul (menangani partial read).
- Batas ukuran pesan: 4 MiB (proteksi dari data rusak).
- Setiap koneksi punya `threading.Lock` untuk `send` (hasil task dan heartbeat bisa
  berasal dari thread berbeda).

---

## 4. Tipe Pesan

| Tipe | Arah | Isi |
| ---- | ---- | --- |
| `HELLO` | W → M | `worker_id`, `hostname`, `cores`, `health_port`, `lan_ip`, `version` |
| `WELCOME` | M → W | `session_id`, `heartbeat_ms`, `task_timeout_ms`, `chunk_size` |
| `HEARTBEAT` | W → M | `load` (proses aktif), `cpu_pct`, `mem_pct` |
| `TASK` | M → W | `task_id`, `targets[]`, `probe`, `attempt` |
| `RESULT` | M → W | `task_id`, `rows[]`, `duration_ms`, `errors` |
| `DRAIN` | M → W | minta worker berhenti mengambil task baru |
| `BYE` | keduanya | penutupan terkontrol |

Contoh `TASK`:

```json
{
  "type": "TASK",
  "task_id": "T0013-21823b",
  "targets": ["10.199.101.5", "10.199.101.6", "... 50 IP ..."],
  "probe": { "kind": "tcp", "port": 80, "count": 4, "timeout": 1.0 },
  "attempt": 0
}
```

Contoh `RESULT`:

```json
{
  "type": "RESULT",
  "task_id": "T0013-21823b",
  "worker_id": "W3-fedora",
  "duration_ms": 1840,
  "rows": [
    {
      "ip": "10.199.101.5",
      "status": "up",
      "sent": 4, "received": 3, "loss_pct": 25.0,
      "rtt_min": 1.2, "rtt_avg": 1.8, "rtt_max": 3.1, "jitter": 0.6,
      "probe": "tcp:80", "ts": 1760000000.0
    }
  ],
  "errors": []
}
```

---

## 5. State Machine

**Worker**

```
START → CONNECTING → HELLO → IDLE ⇄ BUSY → (BYE/ERROR)
                        ▲        │
                        └────────┘  hasil dikirim, kembali IDLE
```

Worker **tidak pernah** meminta task secara eksplisit. Master hanya mengirim `TASK`
saat worker menganggur. Ini menghasilkan *pull behaviour* tanpa pesan tambahan.

**Master (per koneksi worker)**

```
ACCEPT → HANDSHAKE → READY → ASSIGNED → (RESULT → READY) | (TIMEOUT → REQUEUE)
```

| State | Aksi |
| ----- | ---- |
| READY | ambil task dari antrean, kirim `TASK`, pindah `ASSIGNED` |
| ASSIGNED | tunggu `RESULT` sampai `deadline` |
| TIMEOUT | `attempt++`, kembalikan ke antrean (jika `attempt ≤ MAX_RETRIES`) |
| DISCONNECT | kembalikan task in-flight ke antrean, hapus worker |

---

## 6. Penjadwalan & Load Balancing

- **Unit kerja:** 1 task = 1 target untuk IP worker (agar semua node kebagian);
  25–50 IP untuk target dari file/sintetis.
- **Fairness:** `--fairness count` (default) menyeimbangkan jumlah task selesai
  + in-flight; `idle` hanya mendahulukan node READY yang tertinggal
  (work-conserving); `off` murni siapa cepat dia dapat. Pengukuran performa
  memakai `idle` agar throughput tidak tertahan pemerataan.
- **Concurrency per worker:** tepat **1 task** in-flight, karena di dalam worker task
  itu dipecah lagi ke `ProcessPoolExecutor` sejumlah core.
- **Antrean global** `queue.Queue` di master; master hanya memberi task ke worker
  yang `READY` → worker cepat otomatis mengerjakan lebih banyak.
- **Prioritas:** target yang sebelumnya timeout dikirim lebih dulu.
- **Weighted:** worker dengan `load` rendah dan `avg_rtt` kecil diberi chunk
  berukuran lebih besar (mis. `chunk_size × 1.5`).
- **Metrik keseimbangan:** `imbalance = max(waktu_worker) / mean(waktu_worker)`,
  dibandingkan antara mode statis dan dinamis.

Mode statis (pembanding buruk): bagi rata IP ke 9 worker di awal. Karena IP mati
menunggu timeout ±1 s sedangkan IP hidup ±2 ms, satu worker bisa tertinggal jauh.

---

## 7. Fault Tolerance

| Mekanisme | Detail |
| --------- | ------ |
| Heartbeat aplikasi | Worker kirim tiap 2 s; master tandai `DEAD` setelah 6 s tanpa heartbeat |
| TCP keepalive | `SO_KEEPALIVE`, `TCP_KEEPIDLE=5`, `TCP_KEEPINTVL=2`, `TCP_KEEPCNT=3` |
| Task timeout | `count × timeout + 5 s`; lewat batas → requeue |
| Retry | Maksimal 2× (`attempt`), lalu status `FAILED` permanen |
| Idempotensi | Probe tidak mengubah state, aman diulang |
| Ledger | `task_id → {status, attempt, worker, deadline}` disimpan di master (JSON/SQLite) |
| Dedup | Hasil ganda dari retry diabaikan berdasarkan `task_id` |
| Reconnect | Worker backoff 1, 2, 4, 8 detik; `HELLO` ulang dengan `worker_id` sama |

**Recovery time** = selisih waktu antara deteksi `DEAD` dan selesainya task tersebut
di worker lain.

---

## 8. Alur Eksekusi

1. Master `bind` + `listen` di IP `wlp3s0:9998` (default; fallback `0.0.0.0`).
2. 9 worker `connect` ke `10.199.101.109:9998` (default tanpa argumen), kirim
   `HELLO` berisi IP LAN dan health port.
3. Master membalas `WELCOME`, mencatat kapasitas core tiap worker.
4. Setelah node yang dibutuhkan terkumpul (atau `discovery-timeout` habis),
   master menyusun target dari `IP:health_port` setiap worker; jika memakai
   `--target-source file`, target dibaca dari `targets.txt`.
5. Setiap worker `READY` menerima `TASK`; worker memecah target ke proses anak,
   menjalankan probe, mengirim `RESULT`.
6. Master menyimpan baris hasil, mengirim `TASK` berikutnya.
7. Setelah antrean kosong dan semua in-flight selesai, master menulis ringkasan
   (`hasil_ringkas.csv`, `tabel_hasil.md`, grafik) dan mengirim `BYE`.

---

## 9. Data & Konfigurasi

Sumber target **default: IP worker yang terhubung**.

- Worker mengirim `lan_ip` dan `health_port` di `HELLO`; master memakai
  `lan_ip` (wlp3s0) sebagai alamat target, fallback ke IP peer TCP.
- Target probe menjadi `ip:health_port` untuk TCP, atau `ip` untuk ICMP.
- `--repeat` menggandakan jumlah sampel; `--count` mengatur probe per target.
- Tidak ada scan subnet: node yang aktif mendaftar lewat koneksi TCP.
- Task dibagikan ke worker idle; satu target belum tentu diprobe semua node
  (bukan matriks penuh N×N). Kolom `worker` di CSV mencatat pelaksana probe.

`targets.txt` dipakai hanya untuk `--target-source file` atau `both`:

- IP host lab (`10.199.101.x`) — reachable, RTT kecil.
- IP *blackhole* (`192.0.2.x`, TEST-NET-1) — unreachable, memicu timeout.
- Port uji: 22/80/443 (bervariasi), 9999 (pasti tertutup).

Campuran target hidup dan blackhole penting untuk membuktikan load balancing
pada mode file; pada mode IP worker, variasi beban muncul dari perbedaan
latency/CPU antar node.

---

## 10. Struktur File

```
UAS/tema13/
├── common.py       # framing, konstanta, fungsi probe, model pesan
├── master.py       # server 9998, scheduler, ledger, agregasi
├── worker.py       # client, heartbeat, ProcessPoolExecutor
├── baseline.py     # sequential (T1)
├── targets.txt     # opsional (target-source=file)
├── benchmark.py    # matriks uji → CSV
├── ui.py           # form interaktif + notifikasi
├── hasil/
└── laporan/
```

---

## 11. Rencana Pengujian

| # | Skenario | Bukti |
| - | -------- | ----- |
| a | Sequential 1 proses | `T1` baseline |
| b | Local multiprocessing di 1 PC | speedup core |
| c | 1, 2, 4, 6, 9 worker | skalabilitas |
| d | Statis vs dinamis | imbalance, waktu total |
| e | Kill 1–2 worker saat jalan | semua task selesai + recovery time |
| f | Variasi jumlah target | throughput |

Setiap konfigurasi diulang 3×, laporkan rata-rata ± standar deviasi.

---

## 12. Risiko & Mitigasi

| Risiko | Mitigasi |
| ------ | -------- |
| Beban terlalu kecil → speedup < 1 | Gunakan ≥ 5.000 target |
| Semua target hidup → tidak ada imbalance | Campur IP blackhole |
| Firewall master menolak 9998 | `sudo ufw allow 9998/tcp` atau matikan firewall lab |
| Docker bridge mengacaukan deteksi IP | Bind eksplisit ke IP LAN |
| ICMP butuh root | Default probe = TCP connect |
| Worker mati saat menulis hasil | Kirim `RESULT` sekali per task, atomik |
| Master jadi bottleneck | Agregasi O(1) per baris, tulis CSV di akhir |

---

## 13. Etika

Hanya memindai host milik sendiri atau laboratorium yang diizinkan. Jangan mengarahkan
probe ke IP publik pihak lain.
