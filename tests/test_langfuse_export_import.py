from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from agentprof.cli import app
from agentprof.config import DEFAULT_STORE_PATH, AgentProfConfig, PrivacyConfig
from agentprof.ingest.langfuse_export import (
    LangfuseExportFormat,
    load_observations,
    sanitize_observation_payload,
)
from agentprof.store.duckdb_store import DuckDBStore


runner = CliRunner()
FIXTURES = Path(__file__).parent / "fixtures"


def test_load_langfuse_observations_json() -> None:
    observations = load_observations(
        FIXTURES / "langfuse_observations.json",
        file_format=LangfuseExportFormat.json,
    )

    assert len(observations) == 3
    assert observations[0]["id"] == "obs-root-1"


def test_load_langfuse_observations_csv() -> None:
    observations = load_observations(
        FIXTURES / "langfuse_observations.csv",
        file_format=LangfuseExportFormat.csv,
    )

    assert observations == [
        {
            "id": "csv-obs-1",
            "traceId": "trace-csv",
            "parentObservationId": "",
            "type": "SPAN",
            "name": "csv_agent",
            "input": "hello",
            "output": "world",
        }
    ]


def test_load_langfuse_observations_accepts_empty_data_object(tmp_path: Path) -> None:
    export_path = tmp_path / "observations.json"
    export_path.write_text('{"data": []}', encoding="utf-8")

    assert load_observations(export_path) == []


def test_sanitize_preserves_raw_io_only_when_raw_io_is_enabled() -> None:
    config = AgentProfConfig(
        privacy=PrivacyConfig(store_raw_io=True, hash_inputs=False)
    )

    payload = sanitize_observation_payload(
        {
            "id": "obs-1",
            "input": "Authorization: Bearer rawiosecret",
            "metadata": {"Authorization": "Bearer metadatasecret"},
        },
        config=config,
    )

    assert payload["input"] == "Authorization: Bearer rawiosecret"
    assert payload["metadata"]["Authorization"] == "[SECRET]"


def test_import_langfuse_export_stores_sanitized_raw_spans(
    monkeypatch,
) -> None:
    monkeypatch.setenv("AGENTPROF_HASH_SALT", "test-salt")
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        import_result = runner.invoke(
            app,
            [
                "import",
                "langfuse-export",
                "--observations",
                str(FIXTURES / "langfuse_observations.json"),
            ],
        )

        store = DuckDBStore(DEFAULT_STORE_PATH)
        stats = store.stats()
        with store.connect() as connection:
            rows = connection.execute(
                """
                SELECT source_id, trace_id, span_id, parent_span_id, payload_json
                FROM raw_spans
                ORDER BY source_id
                """
            ).fetchall()

        assert init_result.exit_code == 0
        assert import_result.exit_code == 0
        assert "observations imported: 3" in import_result.output
        assert stats["raw_spans"] == 3
        assert rows[0][:4] == ("obs-root-1", "trace-retry-1", "obs-root-1", None)

        payload = rows[0][4]
        parsed_payload = json.loads(payload)
        privacy = parsed_payload["_agentprof_privacy"]

        assert "user@example.com" not in payload
        assert "verysecretvalue12345" not in payload
        assert "nestedsecretvalue12345" not in payload
        assert "cust_ABC123" in payload
        assert "input" not in parsed_payload
        assert "output" not in parsed_payload
        assert privacy["raw_io_stored"] is False
        assert privacy["redacted_io_stored"] is True
        assert privacy["input_hash"] is not None
        assert privacy["output_hash"] is not None
        assert "[EMAIL]" in privacy["input_preview"]
        assert "[SECRET]" in privacy["input_preview"]


def test_import_langfuse_export_requires_hash_salt_when_hashing_enabled(
    monkeypatch,
) -> None:
    monkeypatch.delenv("AGENTPROF_HASH_SALT", raising=False)
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        import_result = runner.invoke(
            app,
            [
                "import",
                "langfuse-export",
                "--observations",
                str(FIXTURES / "langfuse_observations.json"),
            ],
        )

        assert init_result.exit_code == 0
        assert import_result.exit_code == 2
        assert "Cannot hash Langfuse I/O" in import_result.output
        assert not DuckDBStore(DEFAULT_STORE_PATH).stats()["raw_spans"]
