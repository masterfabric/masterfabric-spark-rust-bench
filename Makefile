all: gen gen-large image bench collect test charts

.PHONY: venv gen gen-large image bench collect test charts docs clean-containers
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

collect:
	$(PY) bench/collect.py

test:
	$(PY) -m pytest -q

charts:
	cd viz && ../$(PY) make_charts.py && ../$(PY) make_diagrams.py && ../$(PY) make_hero.py

clean-containers:  ## remove any leftover benchmark containers
	docker ps -aq --filter name=mf-srb- | xargs -r docker rm -f
