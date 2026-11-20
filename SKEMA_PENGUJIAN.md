# Skema Pengujian — Tema 13 Distributed Network Monitoring

Dokumen ini adalah rencana operasional pengujian untuk memenuhi ketentuan tugas:
- **Minimal 10 node** (1 master + 9 worker = 10 perangkat).
- **Minimal 10× pengujian** per skenario, dijalankan dengan opsi `--runs 10` pada `master.py`.
- Default (tanpa `--runs` / tekan `Enter` di form) = hanya **1 kali** jalankan.

File output tiap run ditulis terpisah:
- `hasil/rows_<tag>_rNN.csv`
- `hasil/ringkasan_<tag>_rNN.json`
- `laporan/events_<tag>.jsonl` (semua run, setiap event diberi kolom `"run"`)

---

## 1. Pemetaan ke ketentuan tugas

| Ketentuan tugas | Bagaimana dipenuhi | Bukti |
| --- | --- | --- |
| ≥ 10 node | 1 master + 9 worker = 10 perangkat fisik | `--expected-workers 9`, daftar device di §2 |
| 10× pengujian | `python3 master.py --runs 10 ...` | file `*_r01.json` … `*_r10.json` |
| Dekomposisi tugas | Target (IP worker / sintetis) dibagi menjadi task kecil per worker | `rows_*.csv`, `ringkasan_*.json` |
| Pemrosesan paralel | `ProcessPoolExecutor` per worker (banyak core) | log worker `[TASK]` |
| Pemrosesan terdistribusi | Master–worker TCP, master server 9998, worker klien | log master `[NODE+]`/`[NODES]` |
| Load balancing dinamis | Pull model, 1 task in-flight/worker, `--fairness` | imbalance ratio di ringkasan |
| Fault tolerance | Heartbeat + timeout + requeue + retry | `events_*.jsonl`, uji kill worker |
| Evaluasi performa | duration, throughput, speedup, efficiency, imbalance | `benchmark.py report` |

---

## 2. Topologi dan perangkat

```
       MASTER (1 perangkat)          WORKER (9 perangkat)
  IP: 10.199.101.109              IP: 10.199.101.110 … 10.199.101.118
  port: 9998 (kontrol)            port: 9997 (health, diakses master)
  nama: master                    nama: L1 … L9
```

Master bind ke port `9998/tcp`. Worker connect ke master dan buka health port `9997/tcp` agar master bisa memprobe tiap node.

**Checklist per perangkat sebelum dimulai**
- [ ] Python 3 terpasang, file `master.py`, `worker.py`, `common.py`, `ui.py` tersedia.
- [ ] Firewall izinkan `9998/tcp` di master dan `9997/tcp` di setiap worker (`sudo ufw allow 9998/tcp`, `sudo ufw allow 9997/tcp`).
- [ ] Semua worker mengetahui IP master (`--master <IP-master>` atau env `NETMON_MASTER`).
- [ ] Semua device dalam subnet LAN yang sama (mis. `10.199.101.0/24`).

**Menjalankan worker (di setiap dari 9 mesin):**
```bash
python3 worker.py --master 10.199.101.109 --port 9998 --id L1 --cores 4
```
Ganti `--id L1` … `L9` dan `--cores` sesuai core mesin. Worker tanpa `--id` akan mendapat ID otomatis.

---

## 3. Variabel tetap (controlled variables)

Agar perbandingan antar skenario adil, gunakan nilai berikut ini kecuali skenario tersebut sengaja mengubahnya:

| Parameter | Nilai | Keterangan |
| --- | --- | --- |
| `--probe-port` | 80 | port probe TCP |
| `--count` | 3 | percobaan probe per target |
| `--timeout` | 0.8 | detik |
| `--chunk` | 0 | otomatis (1 untuk IP worker, 50 untuk sintetis) |
| `--discovery-timeout` | 30 | detik menunggu worker |
| `--fairness` | count | kebijakan pembagian task (diubah pada skenario load balancing) |
| `--mode` | dynamic | penjadwalan (diubah pada skenario statis vs dinamis) |
| `--repeat` | 1 | tiap target diambil 1× |
| `--runs` | 10 | pengujian diulang 10× per skenario |
| `--grace` | 2 | jeda sebelum master keluar |

Target: pakai `--synth 5000` untuk uji beban/timeout seragam, atau biarkan default (target = IP 9 worker yang terhubung) untuk uji node nyata.

---

## 4. Matriks 10 pengujian (R01–R10)

Setiap baris dijalankan **10×** dengan `--runs 10`. Tag output menggunakan sufiks `_r01` … `_r10`.

| Kode | Skenario | Perintah master | Worker | Variabel diukur | Tag |
| --- | --- | --- | --- | --- | --- |
| R01 | Sequential baseline (1 proses) | `baseline.py --mode sequential --synth 5000` | – | durasi, throughput | `seq` |
| R02 | Local parallel (1 PC, multicore) | `baseline.py --mode local --cores N --synth 5000` | – | speedup per core | `local_kN` |
| R03 | Distributed 10 node, dynamic | `master.py --synth 5000 --expected-workers 9 --runs 10` | 9 worker, `--cores 4` | waktu, throughput, imbalance | `dist_k9` |
| R04 | Distributed 10 node, static | `master.py --synth 5000 --expected-workers 9 --mode static --runs 10` | 9 worker | waktu vs R03 | `dist_statis_k9` |
| R05 | Load balancing fairness `count` | `master.py --synth 5000 --expected-workers 9 --fairness count --runs 10` | 9 worker | imbalance | `fb_count_k9` |
| R06 | Load balancing fairness `idle` | `master.py --synth 5000 --expected-workers 9 --fairness idle --runs 10` | 9 worker | imbalance | `fb_idle_k9` |
| R07 | Load balancing fairness `off` | `master.py --synth 5000 --expected-workers 9 --fairness off --runs 10` | 9 worker | throughput maksimum | `fb_off_k9` |
| R08 | Fault tolerance: 1 worker mati | `master.py --synth 500 --expected-workers 9 --events … --runs 1` | 9 worker, matikan 1 node saat jalan | detection, recovery | `ft_1` |
| R09 | Fault tolerance: 2 worker mati | `master.py --synth 500 --expected-workers 9 --events … --runs 1` | 9 worker, matikan 2 node saat jalan | detection, recovery | `ft_2` |
| R10 | Skalabilitas: 5 node vs 10 node | `master.py --synth 5000 --expected-workers 4 --runs 10` <br> `master.py --synth 5000 --expected-workers 9 --runs 10` | 4 lalu 9 worker | speedup vs node | `scale_k4`, `scale_k9` |

Catatan:
- R08/R09 menggunakan `--runs 1` karena worker yang mati tidak otomatis kembali; ulangi secara manual jika ingin statistik.
- Untuk uji target IP worker nyata (bukan sintetis), hapus `--synth` dan biarkan default (target = IP worker terhubung).
- Fault tolerance di 10 node fisik dilakukan secara **manual**: jalankan master dengan `--events laporan/events_ft.jsonl`, lalu dari node lain jalankan `ssh <node> 'pkill -f worker.py'` atau tekan `Ctrl+C` pada worker. Master akan mendeteksi via heartbeat timeout, mengembalikan task ke antrean, dan worker lain menyelesaikannya.

---

## 5. Prosedur eksekusi per skenario

1. Nyalakan semua 9 worker di masing-masing mesin, pastikan terhubung ke master.
2. Di master, jalankan perintah sesuai matriks (contoh R03):
   ```bash
   python3 master.py --synth 5000 --expected-workers 9 --runs 10 \
       --discovery-timeout 30 --run-tag dist_k9 --out hasil \
       --events laporan/events_dist_k9.jsonl
   ```
3. Tunggu semua run selesai. Master mencetak `SELESAI tag=dist_k9_rNN` tiap run.
4. Ulangi untuk skenario berikutnya dengan `--run-tag` yang berbeda agar file tidak menimpa.
5. Setelah semua skenario selesai, gabungkan dan analisis:
   ```bash
   python3 benchmark.py report
   ```

---

## 6. Tabel pencatatan dan analisis

Salin hasil `ringkasan_<tag>_rNN.json` ke tabel berikut untuk setiap skenario:

| Run | durasi_s | throughput | speedup | efficiency | imbalance | up | down |
| --- | --- | --- | --- | --- | --- | --- | --- |
| r01 | | | | | | | |
| … | | | | | | | |
| r10 | | | | | | | |
| **mean ± std** | | | | | | | |

Rumus:
- `speedup = T_sequential (R01) / T_K`
- `efficiency = speedup / jumlah_worker`
- `imbalance = max(waktu_sibuk worker) / rata-rata(waktu_sibuk worker)`

`benchmark.py report` mencetak tabel gabungan dari semua `hasil/ringkasan_*.json`, tetapi **belum menghitung mean ± stddev per skenario**. Untuk statistik ulangan, gunakan contoh berikut (disimpan sebagai `analisis.py` atau jalankan langsung):

```python
import json, statistics
from pathlib import Path

def analisis(tag):
    files = sorted(Path("hasil").glob(f"ringkasan_{tag}_r*.json"))
    if not files:
        return
    dur = [json.loads(f.read_text())["duration_s"] for f in files]
    thr = [json.loads(f.read_text())["throughput_targets_s"] for f in files]
    print(f"{tag}: n={len(dur)}")
    print(f"  durasi  : mean={statistics.mean(dur):.2f}s std={statistics.pstdev(dur):.2f}s")
    print(f"  throughput: mean={statistics.mean(thr):.2f} std={statistics.pstdev(thr):.2f} target/detik")

for tag in ["dist_k9", "fb_count_k9", "fb_idle_k9", "fb_off_k9", "scale_k9"]:
    analisis(tag)
```

---

## 7. Validasi dan ancaman validitas

- **Noise jaringan**: jalankan pengujian pada waktu jaringan lab sepi; hindari download besar di subnet yang sama.
- **Jam tidak sinkron**: durasi diukur oleh master masing-masing; perbedaan jam antar device kecil tidak memengaruhi durasi because master mengukur secara lokal.
- **Thermal throttling**: device yang berjalan terlalu lama bisa menurunkan core; beri jeda antara run jika perlu.
- **Etika**: pantau hanya perangkat milik sendiri atau lab yang memberikan izin. Jangan mengarahkan probe ke IP publik pihak lain. Target default = node worker yang sengaja dijalankan untuk sistem ini.

---

## 8. Checklist selesai

- [ ] 10 perangkat siap (1 master + 9 worker), port 9998 dan 9997 dibuka.
- [ ] Semua worker terhubung ke master (`[NODE+]` muncul di master).
- [ ] 10 skenario (R01–R10) dijalankan, masing-masing `--runs 10` (kecuali R08/R09 manual).
- [ ] File `hasil/rows_*_rNN.csv` dan `ringkasan_*_rNN.json` tercatat untuk setiap run.
- [ ] Tabel pencatatan §6 diisi dan mean ± stddev dihitung.
- [ ] Laporan akhir disusun dari `benchmark.py report` + analisis statistik.
