"""Polars, DataFusion and DuckDB must return the same rows for every benchmark query on a small
dataset, read from Parquet and from an Iceberg table holding the same rows."""
import sys

import numpy as np
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, "bench")
import gen_data  # noqa: E402
from collect import same  # noqa: E402
from common import iceberg_metadata, queries  # noqa: E402
from run_python_engine import ENGINES  # noqa: E402

QUERIES = queries("bench/queries.sql")


@pytest.fixture(scope="module")
def data(tmp_path_factory):
    from pyiceberg.catalog.sql import SqlCatalog
    d = tmp_path_factory.mktemp("d")
    rng = np.random.default_rng(1)
    cells, calls = gen_data.cells(rng), gen_data.calls(rng, 0, 200_000)
    pq.write_table(cells, d / "cells.parquet")
    (d / "calls_tiny").mkdir()
    pq.write_table(calls, d / "calls_tiny" / "part-000.parquet")
    cat = SqlCatalog("t", uri=f"sqlite:///{d}/catalog.db", warehouse=f"file://{d}/iceberg")
    cat.create_namespace("bench")
    for name, tbl in (("calls_tiny", calls), ("cells", cells)):
        t = cat.create_table(f"bench.{name}", schema=tbl.schema, location=f"file://{d}/iceberg/bench/{name}")
        half = tbl.num_rows // 2  # two appends, so the reader has to combine two snapshots' files
        t.append(tbl.slice(0, half))
        t.append(tbl.slice(half))
    return d


@pytest.fixture(scope="module")
def reference(data):
    _, run = ENGINES["duckdb"](str(data), "tiny")
    return {q: run(sql) for q, sql in QUERIES.items()}


@pytest.mark.parametrize("fmt", ["parquet", "iceberg"])
@pytest.mark.parametrize("engine", ["polars", "datafusion", "duckdb"])
def test_engines_agree_on_every_query(data, reference, engine, fmt):
    _, run = ENGINES[engine](str(data), "tiny", fmt)
    for q, sql in QUERIES.items():
        rows = run(sql)
        assert rows and same(rows, reference[q]), (engine, fmt, q)


def test_iceberg_metadata_picks_latest_snapshot(data):
    # create (00000), first append (00001), second append (00002): readers must open the last one
    assert iceberg_metadata(str(data), "calls_tiny").rsplit("/", 1)[-1].startswith("00002-")


def test_q6_is_one_week_of_thirty_days():
    sql = QUERIES["q6_one_week"]
    assert "2026-10-08" in sql and "2026-10-15" in sql


def test_same_tolerates_float_order_but_not_value_changes():
    assert same([("a", 1.0000000001)], [("a", 1.0)])
    assert not same([("a", 1.01)], [("a", 1.0)])
    assert not same([("a", 1.0)], [("b", 1.0)])
    assert same([("2026-10-01",)], [("2026-10-01 00:00:00",)])
