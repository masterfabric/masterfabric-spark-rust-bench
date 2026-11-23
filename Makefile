all: gen gen-large image bench collect test charts

.PHONY: venv gen gen-large image bench collect test charts docs clean-containers df-memory ntz-repro
PY = .venv/bin/python

venv:
	uv venv --python 3.12 .venv && uv pip install --python .venv -r requirements.txt

gen:  ## 10M and 50M rows of synthetic call records into data/
	$(PY) bench/gen_data.py 10
	$(PY) bench/gen_data.py 50

gen-large:  ## 100M and 200M rows (about 6.2 GiB more)
	$(PY) bench/gen_data.py 100
	$(PY) bench/gen_data.py 200

image:  ## Polars + DataFusion + DuckDB container
	docker build -t mf-srb/engines:2 -f bench/Dockerfile.engines bench

bench:  ## every engine, one container at a time (6 CPU, 8 GiB); SIZES="10m 50m 100m 200m", ICEBERG_SIZES="50m"
	bash bench/run_all.sh

df-memory:  ## DataFusion memory diagnosis at 200M rows (8 variants, about 6 minutes; needs gen-large and image)
	SIZE=200m bash bench/df_memory.sh

ntz-repro:  ## Spark TIMESTAMP vs TIMESTAMP_NTZ pruning on a small Iceberg table (about 10 s after the image is pulled)
	docker run --rm -v "$(PWD)":/w -w /w --entrypoint python3 tabulario/spark-iceberg:3.5.5_1.8.1 examples/spark_ntz_pruning/repro.py

collect:
	$(PY) bench/collect.py

test:
	$(PY) -m pytest -q

charts:
	cd viz && ../$(PY) make_charts.py && ../$(PY) make_diagrams.py && ../$(PY) make_hero.py

clean-containers:  ## remove any leftover benchmark containers
	docker ps -aq --filter name=mf-srb- | xargs -r docker rm -f
