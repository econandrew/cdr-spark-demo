I run this in a script something like:

```sh
#!/bin/sh

hadoop fs -rm -R <output dir>/*
rm out/*

spark-submit --num-executors 28 --executor-cores 8 --executor-memory 25G \
  --packages com.databricks:spark-csv_2.10:1.2.0 \
  code/process_cdr.py \
  "hdfs://<input dir>/bc_*.csv" \
  "meta/antennas.csv" \
  "hdfs://<output dir>/metrics_counts.csv" \
  "hdfs://<output dir>/metrics.csv" \
  "hdfs://<output dir>/metrics_header.csv"

hadoop fs -getmerge <output dir>/metrics_header.csv out/metrics_header.csv
hadoop fs -getmerge <output dir>/metrics.csv out/metrics.csv
cat out/metrics_header.csv out/metrics.csv > out/metrics_w_header.csv
```
