# Coffee Sales + Google Trends Big Data Pipeline (ETL & ELT)

## Deskripsi Singkat Studi Kasus
Repositori ini berisi implementasi pipeline pengolahan data untuk studi kasus **penjualan coffee shop** yang menggabungkan:
- Data internal transaksi penjualan (CSV dari Kaggle) dengan skala sekitar 149 ribu baris transaksi
- Data eksternal Google Trends (melalui PyTrends) sebagai indikator minat pencarian harian untuk kata kunci `coffee`, `bakery`, `tea`, dan `chocolate` dengan geo `US-NY`

Tujuan analitik utama adalah menghasilkan data terstruktur yang siap dianalisis untuk memahami pola permintaan harian, kontribusi kategori produk, serta keterkaitan tren pencarian digital terhadap performa penjualan.

---

## Arsitektur Sistem (Ringkas)
Arsitektur dibangun dengan pola Data Lake dan Data Warehouse:

1. **Sumber Data**
   - Kaggle (CSV) – transaksi penjualan coffee shop
   - Google Trends (via PyTrends) – interest over time (harian)

2. **Data Lake (Raw Zone)**
   - Menyimpan data mentah tanpa modifikasi
   - Lokasi: `raw/`

3. **Pipeline ETL (Python)**
   - Transform dilakukan sebelum data dimuat ke Data Warehouse
   - Output terkurasi dimuat ke SQLite (Star Schema)
   - Lokasi: `etl_pipeline/`

4. **Pipeline ELT (SQLite + SQL)**
   - Data mentah dimuat terlebih dahulu ke Data Warehouse
   - Transform dilakukan di dalam Data Warehouse menggunakan SQL
   - Lokasi: `elt_pipeline/` dan `warehouse/`

5. **Data Warehouse (SQLite)**
   - ETL: `warehouse/coffee_dw.sqlite`
   - ELT: `warehouse/coffee_dw_elt.sqlite`

6. **Dashboard Analitik**
   - Visualisasi dilakukan menggunakan **Looker Studio** dengan sumber data hasil query atau mart dari Data Warehouse

---

## Perbedaan ETL dan ELT yang Digunakan

### ETL (Extract → Transform → Load)
- **Extract**: mengambil data transaksi (CSV) dan Google Trends (PyTrends) lalu menyimpan sebagai raw di Data Lake
- **Transform (Python)**:
  - pembersihan data (dedup, missing handling)
  - standardisasi tipe data dan datetime
  - feature engineering (misalnya `gross_revenue`, `rev_per_unit`, `date_key`, atribut kalender)
  - integrasi data tren harian dan pembentukan `trend_for_product`
  - validasi kualitas data berbasis aturan (rule-based) dan pencatatan log
- **Load (SQLite)**:
  - membangun Star Schema (`fact_sales`, `dim_date`, `dim_product_category`, `dim_trend`)
  - membentuk mart agregasi untuk analitik

Implikasi: data yang masuk ke warehouse sudah lebih terkurasi dan konsisten untuk kebutuhan dashboard.

### ELT (Extract → Load → Transform)
- **Extract & Load**:
  - data transaksi dan tren dimuat apa adanya ke SQLite sebagai tabel `raw_sales` dan `raw_trends`
- **Transform (SQL di SQLite)**:
  - normalisasi tanggal transaksi yang formatnya bervariasi
  - filtering kategori non-penjualan
  - mapping kategori produk berbasis aturan `LIKE`
  - join ke dimensi tren harian
  - membangun tabel fakta `elt_fact_sales` dan tabel agregasi harian

Implikasi: iterasi definisi transformasi lebih mudah dilakukan lewat SQL, namun kontrol kualitas bergantung pada disiplin query, filter, dan versioning.

---

## Cara Menjalankan Pipeline (Step-by-Step)

### Prasyarat
- Disarankan menjalankan di Google Colab atau environment lokal yang setara.
- Pastikan akses internet tersedia untuk mengambil data dari GitHub raw content dan Google Trends.

### 1) Instal dependensi
```bash
pip install pandas numpy pytrends openpyxl
```

### 2) Jalankan ETL (Python)
Ringkas alur:
1. Extract: unduh CSV transaksi dan ambil Google Trends melalui PyTrends, simpan ke `raw/` dan catat ke log extract.
2. Transform: cleaning, standardisasi, join tren harian, feature engineering, validasi kualitas data, simpan output ke `etl_pipeline/output/`.
3. Load: bangun Star Schema dan muat dimensi serta fakta ke `warehouse/coffee_dw.sqlite`.

Jika menggunakan notebook, jalankan:
- `notebooks/02_etl_pipeline.ipynb`

### 3) Jalankan ELT (Load raw lalu transform di SQLite)
Ringkas alur:
1. Load: muat data transaksi dan tren mentah ke `warehouse/coffee_dw_elt.sqlite` sebagai `raw_sales` dan `raw_trends`.
2. Transform: jalankan SQL untuk membangun dimensi tren harian, tabel fakta, dan tabel agregasi.

Jika menggunakan notebook, jalankan:
- `notebooks/03_elt_pipeline.ipynb`

### 4) Menjalankan Query Analitik
Contoh query analitik disiapkan untuk:
- total revenue
- revenue per bulan
- revenue per kategori
- weekday vs weekend
- top hari dengan revenue tertinggi
- hubungan tren dan revenue harian

Hasil query dapat diekspor ke Excel untuk konsumsi di Looker Studio atau dokumentasi.

### 5) Dashboard Looker Studio
Sumber data dashboard dapat menggunakan:
- tabel fakta dan dimensi (`fact_sales`, `dim_date`, `dim_product_category`, `dim_trend`)
- tabel mart atau agregasi (`mart_daily_category_sales`, `elt_daily_category_sales_trend`, `elt_daily_sales_trend`)

Rekomendasi visual minimal:
- Trend revenue harian
- Revenue per kategori
- Weekday vs weekend
- Rata-rata tren dan revenue harian
- Top 10 hari revenue tertinggi

---

## Tools dan Versi Utama
Versi dapat berbeda tergantung environment. Implementasi ini kompatibel dengan konfigurasi berikut:
- Python 3.9+ (disarankan 3.10+)
- pandas 1.5+ (disarankan 2.x)
- numpy 1.21+
- pytrends (rilis terbaru dari repository PyTrends)
- SQLite 3.x (builtin pada Python)
- openpyxl (ekspor Excel)
- Looker Studio (dashboard)

---

## Catatan Reproducibility
- Pengambilan Google Trends dapat berubah karena sifat data indeks yang bergantung pada periode dan sampling. Untuk menjaga konsistensi eksperimen, file hasil ekstraksi tren disimpan di `raw/` sehingga proses transform dapat diulang dengan input yang sama.
- Seluruh proses pipeline mencatat metadata eksekusi dalam file log JSONL untuk memudahkan audit dan evaluasi.
