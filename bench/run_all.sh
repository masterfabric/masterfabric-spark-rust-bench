#!/usr/bin/env bash
# Run every engine on every size. Containers: --cpus 6 --memory 8g each, one at a time, removed after use.
# Peak memory is the container's cgroup memory.peak; process time is measured from the host and
# includes container and JVM/Python start-up. A run that fails (for example killed for memory) is
# recorded in <name>.exit and the next run starts; it does not stop the benchmark.
#   SIZES="10m 50m 100m 200m"  parquet sizes       ICEBERG_SIZES="50m"  sizes also read from Iceberg
#   ENGINES="polars datafusion duckdb pyspark scala"                    HOST=1  host reference runs
set -uo pipefail
cd "$(dirname "$0")/.."
SPARK_IMG=tabulario/spark-iceberg:3.5.5_1.8.1
ENG_IMG=mf-srb/engines:2
RAW=results/raw; mkdir -p "$RAW"
SIZES=${SIZES:-"10m 50m 100m 200m"}
ICEBERG_SIZES=${ICEBERG_SIZES:-"50m"}
ENGINES=${ENGINES:-"polars datafusion duckdb pyspark scala"}
HOST=${HOST:-1}
LIMITS="--cpus 6 --memory 8g"
now() { python3 -c 'import time; print(time.time())'; }

run() {  # name image cmd
  local name=$1 img=$2 cmd=$3
  local t=$(now)
  rm -f "$RAW/$name.exit" "$RAW/$name.mem"
  docker run --rm --name "mf-srb-$name" $LIMITS -v "$PWD":/w -w /w --entrypoint bash "$img" \
    -c "set -o pipefail; $cmd; rc=\$?; cat /sys/fs/cgroup/memory.peak > $RAW/$name.mem; exit \$rc"
  local rc=$?
  python3 -c "print(round($(now) - $t, 2))" > "$RAW/$name.wall"
  if [ $rc -ne 0 ]; then echo "$rc" > "$RAW/$name.exit"; echo "!! $name exited with $rc"; fi
}

single() {  # engine size format prefix
  run "$4$1_$2" $ENG_IMG "python bench/run_python_engine.py $1 /w/data $2 $RAW/$4$1_$2.json $3"
}

for s in $SIZES; do
  for e in $ENGINES; do
    case $e in
      polars|datafusion|duckdb) single $e $s parquet "" ;;
      pyspark) run "pyspark_$s" $SPARK_IMG "SPARK_CORES=6 SPARK_DRIVER_MEMORY=6g python3 bench/run_pyspark.py /w/data $s $RAW/pyspark_$s.json 2>&1 | grep -v WARN" ;;
      scala) run "scala_$s" $SPARK_IMG "DATA=/w/data SIZE=$s OUT=$RAW/scala_$s.json /opt/spark/bin/spark-shell --master 'local[6]' --driver-memory 6g --conf spark.sql.defaultCatalog=spark_catalog --conf spark.eventLog.enabled=false --conf spark.ui.enabled=false --conf spark.ui.showConsoleProgress=false -i bench/RunSpark.scala 2>&1 | grep -E '^scala '" ;;
    esac
  done
  if [ "$HOST" = 1 ]; then  # reference only: natively on the host (all cores, no container limits)
    for e in polars datafusion duckdb; do
      .venv/bin/python bench/run_python_engine.py $e data $s $RAW/host_${e}_$s.json || echo "!! host $e $s failed"
    done
  fi
done

for s in $ICEBERG_SIZES; do
  [ -f "data/iceberg/bench/calls_$s/metadata/version-hint.text" ] || \
    run "make_iceberg_$s" $SPARK_IMG "python3 bench/make_iceberg.py /w/data $s 2>&1 | grep -v WARN | tee $RAW/make_iceberg_$s.log"
  run "iceberg_plan_$s" $ENG_IMG "python bench/iceberg_plan.py /w/data $s $RAW/iceberg_plan_$s.json"
  for e in polars datafusion duckdb; do single $e $s iceberg "iceberg_"; done
  run "iceberg_pyspark_$s" $SPARK_IMG "FORMAT=iceberg UDF=0 SPARK_CORES=6 SPARK_DRIVER_MEMORY=6g python3 bench/run_pyspark.py /w/data $s $RAW/iceberg_pyspark_$s.json 2>&1 | grep -v WARN"
done
echo done
