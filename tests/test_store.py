from __future__ import annotations

from pathlib import Path

import pytest

from agentprof.store.duckdb_store import TABLES, DuckDBStore, ReportRecord


def test_store_creates_schema(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")

    store.ensure_schema()

    assert store.path.is_file()
    assert store.migrations() == [
        (1, "initial_store_schema"),
        (2, "add_normalized_span_retry_fingerprints"),
        (3, "add_report_html_path"),
    ]


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
    assert store.migrations() == [
        (1, "initial_store_schema"),
        (2, "add_normalized_span_retry_fingerprints"),
        (3, "add_report_html_path"),
    ]


def test_report_html_path_migration_updates_existing_reports_table(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    with store.connect() as connection:
        connection.execute(
            """
            CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE reports (
                report_id TEXT PRIMARY KEY,
                project TEXT,
                window_start TIMESTAMPTZ,
                window_end TIMESTAMPTZ,
                summary_json TEXT NOT NULL DEFAULT '{}',
                report_md_path TEXT,
                report_json_path TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
            )
            """
        )
        connection.executemany(
            "INSERT INTO schema_migrations (version, name) VALUES (?, ?)",
            [
                (1, "initial_store_schema"),
                (2, "add_normalized_span_retry_fingerprints"),
            ],
        )

    store.ensure_schema()

    with store.connect() as connection:
        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info('reports')").fetchall()
        }

    assert store.migrations() == [
        (1, "initial_store_schema"),
        (2, "add_normalized_span_retry_fingerprints"),
        (3, "add_report_html_path"),
    ]
    assert "report_html_path" in columns


def test_store_round_trips_report_html_path(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")

    store.upsert_report(
        ReportRecord(
            report_id="html-report",
            project="tracer",
            window_start=None,
            window_end=None,
            summary={"issue_count": 0},
            report_md_path=".agentprof/reports/html-report.md",
            report_json_path=".agentprof/reports/html-report.json",
            report_html_path=".agentprof/reports/html-report.html",
        )
    )

    reports = store.fetch_reports(report_id="html-report")

    assert len(reports) == 1
    assert reports[0].report_html_path == ".agentprof/reports/html-report.html"


def test_migrations_is_read_only_for_missing_store(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "missing.duckdb")

    with pytest.raises(FileNotFoundError):
        store.migrations()

    assert not store.path.exists()
