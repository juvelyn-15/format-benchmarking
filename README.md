# spark-storage-benchmark

Minimal, reproducible PySpark benchmark comparing storage formats (CSV,
JSON, Parquet, ORC) and partitioning strategies, using the Kaggle
[flight-delays](https://www.kaggle.com/datasets/usdot/flight-delays)
dataset.

- **GitHub** holds source code, configuration, README, and the benchmark
  result files produced by a run (`results/`).
- **Google Colab** is the execution environment (raw dataset + generated
  benchmark datasets are never committed — see `.gitignore`).

## Project structure

```text
spark-storage-benchmark/
├── README.md
├── requirements.txt
├── .gitignore
├── format_benchmark.py        # the only script — runs both experiments
│
├── data/
│   ├── raw/                   # put flights.csv here (git-ignored)
│   └── generated/             # benchmark write outputs (git-ignored)
│
├── results/                   # benchmark_results.csv/json,
│                               # partition_results.csv, file_layout.txt
│
└── logs/                      # benchmark.log
```

## What the script does

`format_benchmark.py` is one PySpark script, run top to bottom:

1. **Load** `data/raw/flights.csv` (configurable path).
2. **Format benchmark** — writes the same DataFrame as CSV, JSON,
   Parquet, and ORC. For each format it records write time, output size
   on disk, and the time to read the data back and run the query:
   ```python
   df.select("AIRLINE", "ARRIVAL_DELAY").groupBy("AIRLINE").avg("ARRIVAL_DELAY")
   ```
   using the dataset's original column names. The query is triggered with
   `.collect()` so Spark actually executes it.
3. **Partition experiment** — writes Parquet three ways and records file
   count, output size, write time, and the resulting file/directory
   layout:
   - `df.repartition(20)`
   - `df.coalesce(2)`
   - `df.write.partitionBy("YEAR", "MONTH")`
4. **Results** are saved to `results/benchmark_results.csv`,
   `results/benchmark_results.json` (also contains run metadata),
   `results/partition_results.csv`, and `results/file_layout.txt`.
5. **Logging** — all progress goes to `logs/benchmark.log` (and stdout)
   via Python's `logging` module.

### Note on column names

The script runs against the Kaggle CSV's original column names —
`AIRLINE`, `ARRIVAL_DELAY`, `YEAR`, `MONTH` — no renaming is applied.
These are set as constants (`CARRIER_COL`, `DELAY_COL`,
`PARTITION_COLS`) at the top of `format_benchmark.py`; update them if
you point the script at a differently-structured dataset.

## Running in Google Colab

```python
# 1. Get the code
!git clone https://github.com/<your-username>/spark-storage-benchmark.git
%cd spark-storage-benchmark

# 2. Install dependencies
!pip install -q -r requirements.txt

# 3. Get the dataset (choose one)
# (a) Kaggle API
!pip install -q kaggle
# upload your kaggle.json first, then:
!kaggle datasets download -d usdot/flight-delays -p data/raw --unzip
!mv data/raw/flights.csv data/raw/flights.csv  # rename if needed
#
# (b) or manually upload flights.csv into data/raw/ via the Colab file browser

# 4. Run the benchmark
!python format_benchmark.py --input data/raw/flights.csv
```

Useful flags:

```text
--input            path to the source CSV (default: data/raw/flights.csv)
--data-dir          where generated benchmark datasets are written (default: data/generated)
--results-dir       where result files are written (default: results)
--logs-dir          where benchmark.log is written (default: logs)
--sample-fraction   e.g. 0.1 to subsample the ~500MB file for faster iteration
--clean-generated   delete data/generated/* after the run finishes
```

## Running locally

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python format_benchmark.py --input data/raw/flights.csv
```

## After a run

Commit the four files under `results/` and `logs/benchmark.log` as the
experimental record for the assignment. `data/raw/` and
`data/generated/` stay local only (see `.gitignore`).
