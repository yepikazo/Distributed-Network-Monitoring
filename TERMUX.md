# Menjalankan di Termux (Android)

Bisa. Termux cocok dipakai sebagai **worker tambahan** atau untuk uji fungsi
master–worker. Untuk angka performa di laporan, tetap pakai laptop/PC.

## 1. Yang perlu diketahui lebih dulu

| Aspek | Kondisi di Android |
| --- | --- |
| Python | Tersedia lewat `pkg install python` (3.12+) |
| Koneksi ke master | Normal, lewat Wi-Fi yang sama |
| Multiprocessing | Umumnya jalan, tetapi sebagian perangkat membatasi; ada fallback `--inline` |
| ICMP (ping) | Sering butuh izin khusus → pakai probe TCP (default) |
| Performa | Tidak representatif: throttling, big.LITTLE, suhu |
| Background | Proses bisa dibunuh Android → perlu wakelock + battery unrestricted |

Kesimpulan: **valid untuk uji fungsional dan fault tolerance**, **bukan** untuk
angka speedup resmi.

---

## 2. Instalasi Termux dan Python

1. Pasang Termux dari **F-Droid** (versi Play Store sudah usang/tidak dirawat).
2. Buka Termux, lalu:

```bash
pkg update && pkg upgrade -y
pkg install -y python git nano
python --version
```

3. (Opsional) `psutil` tidak wajib. Jika gagal di-build, lewati saja — worker
   otomatis berjalan tanpa metrik CPU/memori.

---

## 3. Memindahkan proyek ke HP

Pilih salah satu.

### A. Lewat Git (jika repo sudah di-push)

```bash
cd ~
git clone <URL-REPO> KPT
cd KPT/UAS/tema13
```

### B. Lewat kartu SD / folder Download

Di laptop, zip folder `UAS/tema13`, lalu kirim ke HP (USB, Google Drive,
Telegram, atau `adb push`). Di HP:

```bash
termux-setup-storage          # izinkan akses penyimpanan
cp -r /sdcard/Download/tema13 ~/tema13
cd ~/tema13
```

### C. Lewat SSH dari laptop

Di HP:

```bash
pkg install -y openssh
sshd
whoami                        # catat username
```

Di laptop:

```bash
scp -r UAS/tema13 <user>@<IP-HP>:~/netmon
```

---

## 4. Uji konektivitas ke master

Ganti `10.199.101.109` dengan IP komputer master.

```bash
python - <<'PY'
import socket
s = socket.socket()
s.settimeout(2)
print("hasil:", s.connect_ex(("10.199.101.109", 9998)))
PY
```

- `0` = port terbuka, siap.
- selain `0` = master belum jalan, firewall menutup 9998, atau HP dan master
  berbeda jaringan / Wi-Fi memakai *client isolation*.

Cek IP HP sendiri:

```bash
python - <<'PY'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("8.8.8.8", 80))
print(s.getsockname()[0])
PY
```

---

## 5. Menjalankan HP sebagai worker

```bash
cd ~/tema13
termux-wake-lock
python worker.py --cores 2
```

Worker tanpa argumen langsung menyambung ke master default `10.199.101.109`.
Jika IP master berbeda: `NETMON_MASTER=<IP-master> python worker.py --cores 2`
atau tambahkan `--master <IP-master>`.

Yang harus terlihat:

```
[19:28:45] [WORKER] menyambung ke 10.199.101.109:9998 ...
[19:28:45] [WORKER] health port TCP 9997 di 10.199.101.xx siap diprobe
[19:28:45] [WORKER] TERHUBUNG ke master 10.199.101.109:9998 sebagai android-xxxx (2 core, IP 10.199.101.xx), chunk=auto, health=9997
[19:28:46] [TASK] T0012-...: 5 target (4 up), 210 ms
```

Di master akan muncul:

```
[19:28:45] [NODE+] android-xxxx (localhost, 2 core) dari 10.199.101.55:50216 | total 3/9
[19:28:47] [NODES] 3/9 aktif | L1@10.199.101.12:idle L2@10.199.101.13:sibuk android-xxxx@10.199.101.55:sibuk
```

Hentikan dengan `Ctrl+C`. Master akan mencatat `NODE-` dan mengembalikan task
yang sedang dikerjakan ke antrean.

### Health port untuk diprobe master

Dengan `--target-source workers` (default master), IP HP menjadi target probe
TCP. Worker HP membuka health port **9997**. Uji dari laptop master:

```bash
python3 - <<'PY'
import socket
s = socket.socket(); s.settimeout(2)
print("health:", s.connect_ex(("10.199.101.55", 9997)))   # 0 = siap
PY
```

Jika bukan `0`, kemungkinan Android memblokir koneksi masuk atau port bentrok.
Solusi:

```bash
python worker.py --master 10.199.101.109 --port 9998 --cores 2 --health-port 0
```

Port acak akan dilaporkan ke master. Jika tetap tidak bisa, jalankan master
dengan `--target-source file` dan isi `targets.txt` dengan IP yang memang
boleh dipantau.

### Jika multiprocessing gagal

Gejala: `OSError: [Errno 38] Function not implemented`, proses menggantung, atau
worker langsung mati. Worker akan mencoba fallback otomatis dan mencetak:

```
[WORKER] multiprocessing tidak tersedia (...); memakai mode inline
```

Untuk memaksa:

```bash
python worker.py --master 10.199.101.109 --port 9998 --cores 2 --inline
```

Mode inline menjalankan probe di proses utama. Distribusi antar-node tetap
berjalan; yang hilang hanya paralelisasi di dalam HP.

---

## 6. Menjalankan HP sebagai master (opsional)

```bash
cd ~/tema13
termux-wake-lock
python master.py --synth 200 --expected-workers 2 --interactive
```

Lalu di laptop:

```bash
python worker.py --master <IP-HP> --port 9998 --cores 2
```

Catatan:

- Pastikan HP dan laptop satu Wi-Fi.
- Jika laptop tidak bisa connect, cek apakah Android memblokir koneksi masuk
  (beberapa ROM membatasi); solusi termudah adalah HP sebagai **worker**,
  laptop sebagai master.

---

## 7. Menjaga proses tetap hidup

1. **Wakelock** (setiap sesi):

   ```bash
   termux-wake-lock
   ```

   Lepas setelah selesai:

   ```bash
   termux-wake-unlock
   ```

2. **Optimasi baterai**: Settings → Apps → Termux → Battery → **Unrestricted**
   (nama menu berbeda per merek HP).

3. **Wi-Fi saat tidur**: Settings → Wi-Fi → Advanced → *Keep Wi-Fi on during
   sleep* = Always.

4. **Android 12+ (phantom process killer)**: sistem dapat membunuh proses anak.
   Jika multiprocessing sering mati, sambungkan HP ke PC dengan USB debugging:

   ```bash
   adb shell settings put global settings_enable_monitor_phantom_procs false
   ```

   Ini opsional; jika tidak bisa, gunakan `--inline` + `--cores 1`.

5. Jangan mengunci layar terlalu lama tanpa wakelock; sebagian perangkat tetap
   menidurkan Wi-Fi.

6. **tmux** (opsional) agar sesi tetap hidup saat Termux di background:

   ```bash
   pkg install -y tmux
   tmux new -s netmon
   # lepas: Ctrl+B lalu D
   # kembali: tmux attach -t netmon
   ```

---

## 8. Urutan uji yang disarankan

1. **Mandiri di HP** (tanpa jaringan):

   ```bash
   python baseline.py --synth 20 --count 1 --timeout 0.2 --mode sequential
   ```

2. **HP sebagai worker** ke master laptop, lihat `TERHUBUNG` dan `NODE+`.

3. **Fault tolerance**: hentikan worker HP dengan `Ctrl+C` saat master berjalan;
   pastikan master mencetak `NODE-` dan task selesai di node lain.

4. **Perbandingan**: jalankan skenario yang sama dengan 2 laptop vs
   2 laptop + 1 HP; catat waktu, tetapi jangan jadikan angka utama laporan.

---

## 9. Troubleshooting

| Gejala | Penyebab / solusi |
| --- | --- |
| `Connection refused` | Master belum jalan atau IP/port salah |
| Connect gagal terus | Beda Wi-Fi, AP isolation, atau firewall master menutup 9998 |
| Node HP tampil `down` | Health port 9997 tidak terjangkau; coba `--health-port 0` |
| `Errno 38` / pool hang | Gunakan `--inline` |
| `ping: permission denied` | ICMP dibatasi; gunakan probe TCP (`--probe-kind tcp`) |
| Worker mati saat layar mati | `termux-wake-lock` + battery unrestricted |
| Worker mati tiap beberapa menit | Phantom process killer (lihat bagian 7) |
| `pip install psutil` gagal | Tidak wajib; lewati |
| Hasil lambat / tidak stabil | Normal di HP (throttling, big.LITTLE, suhu) |

---

## 10. Ringkasan perintah

```bash
pkg install -y python git
cd ~/tema13

# sebagai worker
termux-wake-lock
python worker.py --master 10.199.101.109 --port 9998 --cores 2

# fallback tanpa multiprocessing
python worker.py --master 10.199.101.109 --port 9998 --cores 1 --inline

# sebagai master (opsional)
python master.py --synth 200 --expected-workers 2 --interactive
```
