"""
format_benchmark.py

Single-file PySpark benchmark for the "Spark Storage Benchmark" assignment.

It runs two experiments against the Kaggle "flight-delays" dataset
(https://www.kaggle.com/datasets/usdot/flight-delays), specifically the
flights.csv file:

  1. File-format benchmark: write the same DataFrame as CSV / JSON /
     Parquet / ORC, and measure write time, output size, and the time
     to read + run an aggregation query against each format.

  2. Partition experiment: write the DataFrame as Parquet using
     repartition(20), coalesce(2), and partitionBy("YEAR", "MONTH"),
     and record file count, output size, write time, and the resulting
     file/directory layout.

Designed to be run top-to-bottom in Google Colab, or from the CLI:

    python format_benchmark.py --input data/raw/flights.csv

Results are written to results/*.csv, results/*.json and results/file_layout.txt.
Progress is logged to logs/benchmark.log (and stdout).
"""

import argparse
import csv
import json
import logging
import os
import platform
import shutil
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --------------------------------------------------------------------------
# Dataset-specific configuration.
#
# Column names as they actually appear in the Kaggle flight-delays CSV
# (https://www.kaggle.com/datasets/usdot/flight-delays) — no renaming is
# applied, the query below runs against the original columns. Change
# these constants if you point the script at a different dataset.
# --------------------------------------------------------------------------
CARRIER_COL = "AIRLINE"
DELAY_COL = "ARRIVAL_DELAY"
PARTITION_COLS = ["YEAR", "MONTH"]

FORMATS = ["csv", "json", "parquet", "orc"]

logger = logging.getLogger("format_benchmark")


# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------
def setup_logging(logs_dir):
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, "benchmark.log")

    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(log_path, mode="a")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return log_path


# --------------------------------------------------------------------------
# Filesystem helpers
# --------------------------------------------------------------------------
def get_dir_size_bytes(path):
    """Total size of all files under `path` (works for a single file too)."""
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            if os.path.exists(fp):
                total += os.path.getsize(fp)
    return total


def count_data_files(path):
    """Count actual data part-files, ignoring Spark's _SUCCESS / .crc markers."""
    if os.path.isfile(path):
        return 1
    n = 0
    for root, _, files in os.walk(path):
        for f in files:
            if f.startswith("_SUCCESS") or f.endswith(".crc"):
                continue
            n += 1
    return n


def dir_tree_listing(path, max_entries=50):
    """Readable listing of files under `path` with sizes, for file_layout.txt."""
    if not os.path.exists(path):
        return f"{path}/\n  <missing>"

    entries = []
    for root, _, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            rel = os.path.relpath(fp, path)
            entries.append((rel, os.path.getsize(fp)))
    entries.sort()

    lines = [f"{path}/"]
    for rel, size in entries[:max_entries]:
        lines.append(f"  {rel}  ({size:,} bytes)")
    if len(entries) > max_entries:
        lines.append(f"  ... {len(entries) - max_entries} more file(s)")
    lines.append(f"  TOTAL: {len(entries)} file(s), {sum(s for _, s in entries):,} bytes")
    return "\n".join(lines)


def clean_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path)


def warn_if_windows_without_hadoop():
    """On Windows, Spark's local-filesystem writer goes through Hadoop's
    filesystem shim, which needs winutils.exe + hadoop.dll on PATH/HADOOP_HOME
    to do permission checks. Without it, writes fail with:
        UnsatisfiedLinkError: NativeIO$Windows.access0
    This isn't a bug in this script — see README.md's Windows section for
    the fix. We only warn here (don't abort), since some setups configure
    this outside of HADOOP_HOME (e.g. via PATH only)."""
    if platform.system() == "Windows" and not os.environ.get("HADOOP_HOME"):
        logger.warning(
            "Running on Windows without HADOOP_HOME set. If writes fail with "
            "'UnsatisfiedLinkError: NativeIO$Windows.access0', install "
            "winutils.exe + hadoop.dll and set HADOOP_HOME — see the "
            "'Windows setup' section in README.md."
        )


# --------------------------------------------------------------------------
# Data loading + the benchmark query
# --------------------------------------------------------------------------
def load_source_data(spark, input_path, sample_fraction):
    logger.info("Reading source dataset from: %s", input_path)
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

    if sample_fraction < 1.0:
        logger.info("Sampling source data at fraction=%s", sample_fraction)
        df = df.sample(withReplacement=False, fraction=sample_fraction, seed=42)

    cols = df.columns
    if CARRIER_COL not in cols or DELAY_COL not in cols:
        raise ValueError(
            f"Expected columns '{CARRIER_COL}' and '{DELAY_COL}' in the source data. "
            f"Available columns: {cols}. Update CARRIER_COL/DELAY_COL at the top of "
            "this script if your source file uses different names."
        )

    df = df.withColumn(DELAY_COL, F.col(DELAY_COL).cast("double"))
    logger.info("Loaded dataframe with %d columns: %s", len(df.columns), df.columns)
    return df


def run_query(df):
    """The benchmark query used for every format/layout, run against the
    dataset's original column names (see CARRIER_COL / DELAY_COL above)."""
    return df.select(CARRIER_COL, DELAY_COL).groupBy(CARRIER_COL).avg(DELAY_COL)


# --------------------------------------------------------------------------
# Benchmark primitives
# --------------------------------------------------------------------------
def benchmark_write(path, write_fn):
    """Run write_fn() (which must write to `path`), timing it and measuring
    the resulting output on disk."""
    clean_dir(path)
    t0 = time.perf_counter()
    write_fn()
    elapsed = time.perf_counter() - t0
    size_bytes = get_dir_size_bytes(path)
    n_files = count_data_files(path)
    return elapsed, size_bytes, n_files


def benchmark_read_and_query(spark, fmt, path):
    """Time reading the data back and running the fixed aggregation query,
    triggered with .collect() so Spark actually executes the plan."""
    t0 = time.perf_counter()
    if fmt == "csv":
        read_df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    else:
        read_df = spark.read.format(fmt).load(path)
    result = run_query(read_df).collect()
    elapsed = time.perf_counter() - t0
    return elapsed, len(result)


# --------------------------------------------------------------------------
# Experiment 1: file-format benchmark
# --------------------------------------------------------------------------
def run_format_benchmark(spark, df, generated_dir):
    logger.info("=== Starting file-format benchmark: %s ===", ", ".join(FORMATS))
    results = []

    for fmt in FORMATS:
        out_path = os.path.join(generated_dir, f"flights_{fmt}")
        logger.info("[%s] writing to %s", fmt.upper(), out_path)

        def write_fn(fmt=fmt, out_path=out_path):
            writer = df.write.mode("overwrite")
            if fmt == "csv":
                # Without an explicit header, reading the CSV back loses
                # column names entirely — needed for a fair read/query step.
                writer = writer.option("header", True)
            writer.format(fmt).save(out_path)

        write_time, size_bytes, n_files = benchmark_write(out_path, write_fn)
        logger.info(
            "[%s] write_time=%.2fs size=%.2fMB files=%d",
            fmt.upper(), write_time, size_bytes / 1e6, n_files,
        )

        read_time, n_groups = benchmark_read_and_query(spark, fmt, out_path)
        logger.info("[%s] read_query_time=%.2fs result_rows=%d", fmt.upper(), read_time, n_groups)

        results.append(
            {
                "format": fmt,
                "write_time_sec": round(write_time, 4),
                "output_size_bytes": size_bytes,
                "output_size_mb": round(size_bytes / 1e6, 4),
                "num_files": n_files,
                "read_query_time_sec": round(read_time, 4),
            }
        )

    logger.info("=== File-format benchmark complete ===")
    return results


# --------------------------------------------------------------------------
# Experiment 2: partition experiment (Parquet only)
# --------------------------------------------------------------------------
def run_partition_experiment(spark, df, generated_dir):
    logger.info("=== Starting partition experiment (Parquet) ===")

    strategies = {
        "repartition_20": lambda d: d.repartition(20),
        # NOTE: coalesce() can only *reduce* partition count. If the source
        # DataFrame already has fewer partitions than requested here (common
        # on small/sample files), the output will have fewer files than 2 —
        # that's expected Spark behavior, not a bug.
        "coalesce_2": lambda d: d.coalesce(2),
        "partitionBy_year_month": lambda d: d,  # partitioning happens at write time below
    }

    results = []
    layout_sections = []

    for name, transform in strategies.items():
        out_path = os.path.join(generated_dir, f"partition_{name}")
        logger.info("[%s] writing to %s", name, out_path)

        def write_fn(name=name, transform=transform, out_path=out_path):
            transformed = transform(df)
            writer = transformed.write.mode("overwrite")
            if name == "partitionBy_year_month":
                writer = writer.partitionBy(*PARTITION_COLS)
            writer.parquet(out_path)

        write_time, size_bytes, n_files = benchmark_write(out_path, write_fn)
        logger.info(
            "[%s] write_time=%.2fs size=%.2fMB files=%d",
            name, write_time, size_bytes / 1e6, n_files,
        )

        results.append(
            {
                "strategy": name,
                "num_files": n_files,
                "output_size_bytes": size_bytes,
                "output_size_mb": round(size_bytes / 1e6, 4),
                "write_time_sec": round(write_time, 4),
            }
        )

        layout_sections.append(f"### Strategy: {name}\n" + dir_tree_listing(out_path))

    logger.info("=== Partition experiment complete ===")
    return results, "\n\n".join(layout_sections)


# --------------------------------------------------------------------------
# Metadata + results output
# --------------------------------------------------------------------------
def collect_metadata(spark, input_path, sample_fraction, row_count):
    input_size = get_dir_size_bytes(input_path) if os.path.exists(input_path) else None
    return {
        "dataset": "usdot/flight-delays (Kaggle)",
        "input_path": input_path,
        "input_size_bytes": input_size,
        "input_size_mb": round(input_size / 1e6, 4) if input_size else None,
        "sample_fraction": sample_fraction,
        "row_count": row_count,
        "spark_version": spark.version,
        "python_version": platform.python_version(),
        "execution_mode": spark.sparkContext.master,
        "benchmark_date_utc": datetime.now(timezone.utc).isoformat(),
    }


def write_results(results_dir, metadata, format_results, partition_results, layout_report):
    os.makedirs(results_dir, exist_ok=True)

    csv_path = os.path.join(results_dir, "benchmark_results.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(format_results[0].keys()))
        writer.writeheader()
        writer.writerows(format_results)
    logger.info("Wrote %s", csv_path)

    json_path = os.path.join(results_dir, "benchmark_results.json")
    with open(json_path, "w") as f:
        json.dump({"metadata": metadata, "format_results": format_results}, f, indent=2)
    logger.info("Wrote %s", json_path)

    part_csv_path = os.path.join(results_dir, "partition_results.csv")
    with open(part_csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(partition_results[0].keys()))
        writer.writeheader()
        writer.writerows(partition_results)
    logger.info("Wrote %s", part_csv_path)

    layout_path = os.path.join(results_dir, "file_layout.txt")
    with open(layout_path, "w") as f:
        f.write("Partition experiment file/directory layout\n")
        f.write("=" * 50 + "\n\n")
        f.write(layout_report + "\n")
    logger.info("Wrote %s", layout_path)


# --------------------------------------------------------------------------
# CLI + main
# --------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Spark storage-format & partition benchmark")
    p.add_argument("--input", default=r"format_benchmark\spark-storage-benchmark\data\raw\flights_1mb.csv", help="Path to source CSV")
    p.add_argument("--data-dir", default=r"format_benchmark\spark-storage-benchmark\data\generated", help="Where benchmark datasets are written")
    p.add_argument("--results-dir", default=r"format_benchmark\spark-storage-benchmark\results", help="Where result files are written")
    p.add_argument("--logs-dir", default=r"format_benchmark\spark-storage-benchmark\logs", help="Where the log file is written")
    p.add_argument(
        "--sample-fraction",
        type=float,
        default=1.0,
        help="Optional fraction in (0, 1] to subsample the input for faster iteration",
    )
    p.add_argument(
        "--clean-generated",
        action="store_true",
        help="Delete generated benchmark datasets after the run (they are git-ignored either way)",
    )
    return p.parse_args()


def main():
    args = parse_args()
    setup_logging(args.logs_dir)
    logger.info("Starting Spark storage-format benchmark")
    warn_if_windows_without_hadoop()

    spark = (
        SparkSession.builder.appName("SparkStorageBenchmark")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    try:
        df = load_source_data(spark, args.input, args.sample_fraction)
        df.cache()
        row_count = df.count()
        logger.info("Source dataframe row count: %d", row_count)

        metadata = collect_metadata(spark, args.input, args.sample_fraction, row_count)
        logger.info("Experiment metadata: %s", metadata)

        format_results = run_format_benchmark(spark, df, args.data_dir)
        partition_results, layout_report = run_partition_experiment(spark, df, args.data_dir)

        write_results(args.results_dir, metadata, format_results, partition_results, layout_report)

        if args.clean_generated:
            logger.info("Cleaning up generated datasets in %s", args.data_dir)
            clean_dir(args.data_dir)

        logger.info("Benchmark finished successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
