"""Polars and DataFusion must return the same rows for every benchmark query on a small dataset."""
import sys
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, "bench")
import gen_data  # noqa: E402
from collect import same  # noqa: E402
from common import queries  # noqa: E402
from run_python_engine import datafusion_engine, polars_engine  # noqa: E402


@pytest.fixture(scope="module")
def data(tmp_path_factory):
    d = tmp_path_factory.mktemp("d")
    rng = np.random.default_rng(1)
    pq.write_table(gen_data.cells(rng), d / "cells.parquet")
    (d / "calls_tiny").mkdir()
    pq.write_table(gen_data.calls(rng, 0, 200_000), d / "calls_tiny" / "part-000.parquet")
    return d


@pytest.mark.parametrize("name", list(queries("bench/queries.sql")))
def test_polars_and_datafusion_agree(data, name):
    sql = queries("bench/queries.sql")[name]
    _, pl_run = polars_engine(str(data), "tiny")
    _, df_run = datafusion_engine(str(data), "tiny")
    a, b = pl_run(sql), df_run(sql)
    assert a and same(a, b)


def test_same_tolerates_float_order_but_not_value_changes():
    assert same([("a", 1.0000000001)], [("a", 1.0)])
    assert not same([("a", 1.01)], [("a", 1.0)])
    assert not same([("a", 1.0)], [("b", 1.0)])
    assert same([("2026-10-01",)], [("2026-10-01 00:00:00",)])
