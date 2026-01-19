# Simple Weather Data ELT Pipeline 🌦️

![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=flat&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?style=flat&logo=apache-airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Container-Docker-2496ED?style=flat&logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/Staging-PostgreSQL-4169E1?style=flat&logo=postgresql&logoColor=white)
![Google BigQuery](https://img.shields.io/badge/Data%20Warehouse-BigQuery-4285F4?style=flat&logo=google-cloud&logoColor=white)
![dbt](https://img.shields.io/badge/Transformation-dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![Looker](https://img.shields.io/badge/BI-Looker-4285F4?style=flat&logo=looker&logoColor=white)

## 📋 Summary Proyek

Repository ini adalah implementasi **Simple ELT Pipeline** untuk data cuaca.

Proyek ini mendemonstrasikan cara membangun *modern data flow* secara end-to-end: mengambil *raw data* dari public API, memuatnya ke cloud Data Warehouse, melakukan transformasi data yang kompleks menggunakan dbt, dan akhirnya menyajikan insight tersebut melalui dashboard interaktif di **Google Looker**.

Seluruh workflow diorkestrasi secara otomatis menggunakan **Apache Airflow** yang berjalan di dalam Docker container.

Pipeline ini menggunakan pendekatan **ELT (Extract, Load, Transform)**. Data akan dimuat (*loaded*) dalam bentuk mentahnya ke Data Warehouse terlebih dahulu, dan baru ditransformasikan kemudian agar siap dikonsumsi oleh BI tool.

Berikut adalah hasil visualisasi sementara: https://lookerstudio.google.com/reporting/16ad358d-baa5-4702-a3db-e90aea4e47bf

## 🛠️ Tech Stack

Pipeline ini dibangun menggunakan kombinasi teknologi *open-source* dan cloud yang standar di industri:

| Kategori | Teknologi | Deskripsi & Peran |
| --- | --- | --- |
| **Orchestration** | **Apache Airflow** | Mengatur jadwal dan memonitor eksekusi seluruh *tasks* dalam pipeline (DAGs) untuk memastikan urutan jalannya benar. |
| **Ingestion (E&L)** | **Python (Pandas, Requests)** | Custom scripts untuk *extract* data dari 3rd party API dan menangani *initial load* ke local database. |
| **Local Staging** | **PostgreSQL** | Local database sementara (di dalam Docker) yang berfungsi sebagai *staging area* untuk data mentah sebelum dikirim ke cloud secara batch. |
| **Data Warehouse** | **Google BigQuery** | Serverless Data Warehouse untuk menyimpan *raw data* secara permanen dan tempat menjalankan semua proses transformasi dbt. |
| **Transformation (T)** | **dbt (Data Build Tool)** | Menangani logika transformasi data menggunakan SQL yang modular di dalam BigQuery, termasuk *testing* dan dokumentasi model. |
| **BI / Visualization** | **Google Looker** | BI Platform yang connect ke BigQuery untuk visualisasi data dan pembuatan dashboard interaktif bagi *end-users*. |
| **Infrastructure** | **Docker & Docker Compose** | Membungkus semua *services* (Airflow, Postgres) agar *local development environment* terisolasi dan konsisten. |

---

## 🧅 Lapisan Transformasi Data (dbt Models)

Proses transformasi ("T" dalam ELT) dilakukan secara berlapis (*layered approach*) di dalam BigQuery menggunakan dbt untuk memastikan modularitas dan *data quality*:

### 1. Raw Data Layer (Source)

* Data mendarat di BigQuery dalam format aslinya (JSON/String mentah) dari API. Layer ini bersifat *immutable* (tidak boleh diubah) sebagai *history* data asli.

### 2. Staging Layer (`stg_`)

* **Fungsi:** Fokus pada *cleaning* dan standarisasi *raw data*.
* **Aktivitas:** Mengubah tipe data (*type casting* string ke timestamp/numeric), mem-parsing kolom JSON yang kompleks, dan menstandarkan nama kolom menjadi `snake_case`.

### 3. Intermediate Layer (`int_`)

* **Fungsi:** Melakukan agregasi dan perhitungan di tengah proses (*mid-process calculations*).
* **Aktivitas:** Mengubah *grain* data (misalnya, dari per jam menjadi ringkasan harian) dan menghitung window functions (seperti *7-day moving average* untuk melihat tren).

### 4. Mart Layer (`mart_`)

* **Fungsi:** Tabel final yang sudah dioptimalkan untuk konsumsi **Looker**.
* **Aktivitas:** Menerapkan *final business logic* (contoh: membuat flag "Cuaca Baik untuk Lari") dan menyajikan data dengan struktur yang efisien untuk *query performance* di dashboard.

---

## 📂 Struktur Main Project

```bash
.
├── airflow/
│   ├── dags/                           # Definisi DAG Airflow untuk orkestrasi pipeline
│   └── Dockerfile                      # Custom image Airflow dengan dbt terinstal
├── api_request/                        # Script Python untuk Ingestion (Extract & Load)
│   ├── api_request.py                  # Main script untuk fetch API
│   └── postgres_to_bq.py               # Script untuk load data dari local Postgres ke BigQuery
├── dbt/
│   └── my_project/                     # Project root dbt
│       ├── models/
│       │   ├── staging/                # Model SQL layer Staging
│       │   ├── intermediate/           # Model SQL layer Intermediate
│       │   └── mart/                   # Model SQL layer Mart (siap untuk Looker)
│       └── dbt_project.yml             # Konfigurasi utama project dbt
├── postgres/                           # Script inisialisasi untuk database Postgres lokal
└── docker-compose.yaml                 # Definisi seluruh services infrastruktur (Airflow, DB)

```

