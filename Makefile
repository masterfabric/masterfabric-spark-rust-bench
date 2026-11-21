all: gen image bench collect test charts

.PHONY: venv gen image bench collect test charts docs clean-containers
PY = .venv/bin/python

venv:
	uv venv --python 3.12 .venv && uv pip install --python .venv -r requirements.txt

gen:  ## 10M and 50M rows of synthetic call records into data/
	$(PY) bench/gen_data.py 10
	$(PY) bench/gen_data.py 50

image:  ## Polars + DataFusion container
	docker build -t mf-srb/rust-engines:1 -f bench/Dockerfile.rust-engines bench

bench:  ## every engine, one container at a time (6 CPU, 8 GiB); SIZES="10m 50m" by default
	bash bench/run_all.sh

collect:
	$(PY) bench/collect.py

test:
	$(PY) -m pytest -q

charts:
	cd viz && ../$(PY) make_charts.py && ../$(PY) make_diagrams.py && ../$(PY) make_hero.py

clean-containers:  ## remove any leftover benchmark containers
	docker ps -aq --filter name=mf-srb- | xargs -r docker rm -f
