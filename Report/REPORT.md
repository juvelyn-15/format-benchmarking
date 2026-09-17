# Comparative Storage Benchmark Report

## 1. Objective

This benchmark compares the storage efficiency and query-read performance of four common data formats: CSV, JSON, Parquet, and ORC. It also evaluates how Parquet output changes under different partitioning strategies: `repartition(20)`, `coalesce(2)`, and `partitionBy("YEAR", "MONTH")`.

## 2. Dataset and Environment

The benchmark used the Kaggle `usdot/flight-delays` dataset, specifically the `flights.csv` file.

| Item | Value |
|---|---:|
| Input file | `format_benchmark\spark-storage-benchmark\data\raw\flights.csv` |
| Input size | 592,406,591 bytes / 592.4066 MB |
| Rows processed | 5,819,079 |
| Sample fraction | 1.0 |
| Spark version | 3.5.9 |
| Python version | 3.11.7 |
| Spark execution mode | `local[2]` |
| Benchmark timestamp, UTC | 2026-09-17T05:31:13.315023+00:00 |

The benchmark script is `format_v2.py`. It reads the source CSV, writes the same DataFrame to CSV, JSON, Parquet, and ORC, then reads each format back and runs the equivalent of:

```sql
SELECT AIRLINE AS carrier, AVG(ARRIVAL_DELAY) AS avg_arr_delay
FROM flights
GROUP BY AIRLINE;
```

The assignment query refers to `carrier` and `arr_delay`; in the Kaggle flight-delays dataset these fields are named `AIRLINE` and `ARRIVAL_DELAY`.

## 3. Measurement Method

For each file format, the script recorded:

- Disk storage size: total bytes under the Spark output directory, including Spark marker/CRC files.
- Number of data files: Spark data part-files only, excluding `_SUCCESS` and `.crc` files.
- Write execution time: elapsed wall-clock time for the write operation.
- Read query time: elapsed wall-clock time to read the dataset and execute the aggregation query with `.collect()`.

Sizes are reported in decimal megabytes, where `1 MB = 1,000,000 bytes`.

## 4. Format Benchmark Results

| Format | Write Time (s) | Size (bytes) | Size (MB) | Data Files | Read Query Time (s) |
|---|---:|---:|---:|---:|---:|
| CSV | 9.3927 | 605,797,497 | 605.7975 | 5 | 14.2595 |
| JSON | 17.0457 | 2,736,443,483 | 2736.4435 | 5 | 29.0112 |
| Parquet | 8.7478 | 142,627,174 | 142.6272 | 5 | 0.8182 |
| ORC | 10.1279 | 177,687,315 | 177.6873 | 5 | 1.7012 |

## 5. Format Comparison

Parquet produced the smallest output and the fastest read-query result. Its output was 142.6272 MB, which is about 23.5% of the CSV output size and about 5.2% of the JSON output size. Parquet also had the fastest write time at 8.7478 seconds.

ORC was the second-smallest and second-fastest analytical format. It used 177.6873 MB and completed the read query in 1.7012 seconds. This was slower than Parquet in this run, but still much faster than CSV and JSON for the column-selective aggregation.

CSV was larger and much slower to query than Parquet or ORC. Its read query took 14.2595 seconds because Spark had to parse row-oriented text and infer/read the schema from CSV.

JSON had the largest output and slowest query time. Its 2736.4435 MB output was more than 4.5 times the CSV output size, and the read query took 29.0112 seconds.

## 6. Partition Control Experiment

The partition experiment wrote the same dataset as Parquet using three strategies.

| Strategy | Data Files | Size (bytes) | Size (MB) | Write Time (s) |
|---|---:|---:|---:|---:|
| `repartition(20)` | 20 | 159,373,327 | 159.3733 | 25.1863 |
| `coalesce(2)` | 2 | 143,032,196 | 143.0322 | 8.7376 |
| `partitionBy("YEAR", "MONTH")` | 16 | 143,801,211 | 143.8012 | 9.4774 |

### Partition Findings

`repartition(20)` created 20 data files and had the highest write time at 25.1863 seconds. This is expected because `repartition` performs a shuffle to redistribute rows across the requested number of partitions. It also produced a larger total output size than the other Parquet partition strategies.

`coalesce(2)` created 2 data files and had the lowest partition-experiment write time at 8.7376 seconds. Because `coalesce` reduces partitions with less shuffle overhead, it was faster than `repartition(20)` in this benchmark.

`partitionBy("YEAR", "MONTH")` created a directory layout by `YEAR` and `MONTH`, producing 16 data files across the available year-month partitions. Its total size, 143.8012 MB, was close to the `coalesce(2)` output size, and its write time was 9.4774 seconds.

## 7. Conclusion

For this dataset and query pattern, Parquet was the best overall format. It had the smallest storage footprint, fastest write time, and fastest read-query time. ORC also performed well and substantially outperformed CSV and JSON for analytical reads. CSV remained useful as an interoperable plain-text format, but it was slower and larger than the columnar formats. JSON was the least efficient format in both storage size and query speed.

For Parquet partition control, `coalesce(2)` was the fastest and smallest in this run, while `repartition(20)` created more files and paid a significant shuffle cost. `partitionBy("YEAR", "MONTH")` is useful when downstream queries filter by year or month, because the directory layout enables partition pruning, but this benchmark only measured write layout and not a year/month-filtered read query.

