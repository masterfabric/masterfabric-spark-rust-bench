#!/usr/bin/env bash
# Run every engine on every size. Containers: --cpus 6 --memory 8g each, one at a time, removed after use.
# Peak memory is the container's cgroup memory.peak; process time is measured from the host and
# includes container and JVM/Python start-up.
set -euo pipefail
cd "$(dirname "$0")/.."
SPARK_IMG=tabulario/spark-iceberg:3.5.5_1.8.1
RUST_IMG=mf-srb/rust-engines:1
RAW=results/raw; mkdir -p "$RAW"
SIZES=${SIZES:-"10m 50m"}
LIMITS="--cpus 6 --memory 8g"
now() { python3 -c 'import time; print(time.time())'; }

run() {  # name image entry cmd...
  local name=$1 img=$2 entry=$3; shift 3
  local t=$(now)
  docker run --rm --name "mf-srb-$name" $LIMITS -v "$PWD":/w -w /w --entrypoint "$entry" "$img" \
    -c "$* && cat /sys/fs/cgroup/memory.peak > $RAW/$name.mem"
  python3 -c "print(round($(now) - $t, 2))" > "$RAW/$name.wall"
}

for s in $SIZES; do
  run "polars_$s" $RUST_IMG bash "python bench/run_python_engine.py polars /w/data $s $RAW/polars_$s.json"
  run "datafusion_$s" $RUST_IMG bash "python bench/run_python_engine.py datafusion /w/data $s $RAW/datafusion_$s.json"
  run "pyspark_$s" $SPARK_IMG bash "SPARK_CORES=6 SPARK_DRIVER_MEMORY=6g python3 bench/run_pyspark.py /w/data $s $RAW/pyspark_$s.json 2>&1 | grep -v WARN"
  run "scala_$s" $SPARK_IMG bash "DATA=/w/data SIZE=$s OUT=$RAW/scala_$s.json /opt/spark/bin/spark-shell --master 'local[6]' --driver-memory 6g --conf spark.sql.defaultCatalog=spark_catalog --conf spark.eventLog.enabled=false --conf spark.ui.enabled=false --conf spark.ui.showConsoleProgress=false -i bench/RunSpark.scala 2>&1 | grep -E '^scala '"
  # reference only: Polars and DataFusion natively on the host (all 14 cores, no container limits)
  .venv/bin/python bench/run_python_engine.py polars data $s $RAW/host_polars_$s.json
  .venv/bin/python bench/run_python_engine.py datafusion data $s $RAW/host_datafusion_$s.json
done
echo done
