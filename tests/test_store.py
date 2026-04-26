from __future__ import annotations

from pathlib import Path

from agentprof.store.duckdb_store import TABLES, DuckDBStore


def test_store_creates_schema(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")

    store.ensure_schema()

    assert store.path.is_file()
    assert store.migrations() == [(1, "initial_store_schema")]


def test_store_stats_include_all_tables(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")

    stats = store.stats()

    assert set(stats) == set(TABLES)
    assert all(rows == 0 for rows in stats.values())


def test_store_reset_recreates_schema(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    store.ensure_schema()

    store.reset()

    assert store.path.is_file()
    assert store.migrations() == [(1, "initial_store_schema")]
