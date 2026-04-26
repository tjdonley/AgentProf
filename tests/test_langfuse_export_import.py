from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from agentprof.cli import app
from agentprof.config import DEFAULT_STORE_PATH, AgentProfConfig, PrivacyConfig
from agentprof.ingest.langfuse_export import (
    LangfuseExportFormat,
    LangfuseExportImportError,
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


def test_load_langfuse_observations_wraps_malformed_json(tmp_path: Path) -> None:
    export_path = tmp_path / "observations.json"
    export_path.write_text('{"data": [', encoding="utf-8")

    try:
        load_observations(export_path)
    except LangfuseExportImportError as exc:
        assert "Could not parse Langfuse observations JSON" in str(exc)
    else:
        raise AssertionError("expected LangfuseExportImportError")


def test_load_langfuse_observations_wraps_malformed_csv(tmp_path: Path) -> None:
    export_path = tmp_path / "observations.csv"
    export_path.write_text('id,traceId\n"unterminated,trace-1\n', encoding="utf-8")

    try:
        load_observations(export_path, file_format=LangfuseExportFormat.csv)
    except LangfuseExportImportError as exc:
        assert "Could not parse Langfuse observations CSV" in str(exc)
    else:
        raise AssertionError("expected LangfuseExportImportError")


def test_import_langfuse_export_reports_malformed_json(tmp_path: Path) -> None:
    export_path = tmp_path / "observations.json"
    export_path.write_text('{"data": [', encoding="utf-8")

    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        import_result = runner.invoke(
            app,
            [
                "import",
                "langfuse-export",
                "--observations",
                str(export_path),
            ],
        )

        assert init_result.exit_code == 0
        assert import_result.exit_code == 2
        assert "Could not import Langfuse export" in import_result.output
        assert "Could not parse Langfuse observations JSON" in import_result.output


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


def test_sanitize_stores_retry_fingerprints_for_hashed_io(monkeypatch) -> None:
    monkeypatch.setenv("AGENTPROF_HASH_SALT", "test-salt-value-123")
    config = AgentProfConfig()

    first = sanitize_observation_payload(
        {
            "id": "obs-1",
            "input": {"query": "refund", "request_id": "req-1"},
        },
        config=config,
    )
    second = sanitize_observation_payload(
        {
            "id": "obs-2",
            "input": {"query": "refund", "request_id": "req-2"},
        },
        config=config,
    )

    first_privacy = first["_agentprof_privacy"]
    second_privacy = second["_agentprof_privacy"]

    assert first_privacy["input_hash"] != second_privacy["input_hash"]
    assert first_privacy["input_retry_fingerprint"] is not None
    assert first_privacy["input_retry_fingerprint"] == second_privacy["input_retry_fingerprint"]


def test_sanitize_hashes_user_id_when_hashing_enabled(monkeypatch) -> None:
    monkeypatch.setenv("AGENTPROF_HASH_SALT", "test-salt-value-123")
    config = AgentProfConfig()

    payload = sanitize_observation_payload(
        {
            "id": "obs-1",
            "sessionId": "raw-session-1",
            "userId": "raw-user-1",
        },
        config=config,
    )

    serialized = json.dumps(payload, sort_keys=True)
    privacy = payload["_agentprof_privacy"]
    user_hash = privacy["user_hash"]
    session_hash = privacy["session_hash"]

    assert user_hash is not None
    assert user_hash != "raw-user-1"
    assert len(user_hash) == 64
    assert session_hash is not None
    assert session_hash != "raw-session-1"
    assert len(session_hash) == 64
    assert "raw-user-1" not in serialized
    assert "raw-session-1" not in serialized
    assert "userId" not in payload
    assert "sessionId" not in payload


def test_import_langfuse_export_stores_sanitized_raw_spans(
    monkeypatch,
) -> None:
    monkeypatch.setenv("AGENTPROF_HASH_SALT", "test-salt-value-123")
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
        assert privacy["input_retry_fingerprint"] is not None
        assert privacy["output_retry_fingerprint"] is not None
        assert "[EMAIL]" in privacy["input_preview"]
        assert "[SECRET]" in privacy["input_preview"]


def test_import_langfuse_export_removes_raw_identity_fields(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("AGENTPROF_HASH_SALT", "test-salt-value-123")
    observations_path = tmp_path / "observations.json"
    observations_path.write_text(
        json.dumps(
            [
                {
                    "id": "obs-identity-1",
                    "traceId": "trace-identity-1",
                    "type": "SPAN",
                    "name": "support_agent",
                    "sessionId": "raw-session-1",
                    "userId": "raw-user-1",
                }
            ]
        ),
        encoding="utf-8",
    )

    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        import_result = runner.invoke(
            app,
            [
                "import",
                "langfuse-export",
                "--observations",
                str(observations_path),
            ],
        )

        store = DuckDBStore(DEFAULT_STORE_PATH)
        payload = store.fetch_raw_spans()[0].payload_json

    parsed_payload = json.loads(payload)
    privacy = parsed_payload["_agentprof_privacy"]

    assert init_result.exit_code == 0
    assert import_result.exit_code == 0
    assert "raw-user-1" not in payload
    assert "raw-session-1" not in payload
    assert "userId" not in parsed_payload
    assert "sessionId" not in parsed_payload
    assert privacy["user_hash"] is not None
    assert privacy["session_hash"] is not None


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
        assert "Cannot hash Langfuse identifiers or I/O" in import_result.output
        assert not DuckDBStore(DEFAULT_STORE_PATH).stats()["raw_spans"]


def test_import_langfuse_export_rejects_weak_hash_salt(monkeypatch) -> None:
    monkeypatch.setenv("AGENTPROF_HASH_SALT", "short")
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
        assert "at least" in import_result.output
        assert not DuckDBStore(DEFAULT_STORE_PATH).stats()["raw_spans"]
