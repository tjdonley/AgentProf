<p align="center">
  <img src="assets/agentprof-logo.svg" alt="AgentProf logo" width="520">
</p>

# AgentProf

AgentProf is a local-first CLI for inspecting AI-agent traces and turning them into failure-and-waste signals.

The current MVP imports Langfuse observation exports, sanitizes persisted payloads, normalizes spans/traces into DuckDB, runs deterministic analyzers, builds a cost ledger waterfall from normalized span costs, and generates local Markdown/JSON reports. The longer-term goal is broader source support, more analyzers, and polished shareable reports that explain what failed, what wasted money, and what to fix.

This repository is early MVP software. It is usable for local Langfuse export import, normalization, analysis, cost attribution, and report generation. Additional sources, analyzers, and report formats are still being built.

## What Works Today

- Initialize a local AgentProf workspace with `agentprof init`.
- Import Langfuse observations from JSON or CSV exports.
- Redact common secrets and PII before raw payloads are persisted.
- Hash input/output values with HMAC-SHA256 when configured.
- Store raw and normalized trace data in a local DuckDB database.
- Normalize Langfuse observations into canonical span and trace tables.
- Show data-quality coverage for parent links, status, costs, tokens, models, and I/O hashes.
- Build an idempotent cost ledger from normalized span costs.
- Print a status-based cost waterfall for successful, failed, and unknown span costs.
- Detect retry loops where the same failing call repeats with the same input fingerprint and error signature.
- Detect configured tool/spec contract violations from normalized redacted previews and error messages.
- Generate local Markdown and JSON reports from persisted issues, evidence, and costs.
- List and show generated reports from the local store.

## Planned / Not Built Yet

- Additional deterministic failure/waste analyzers beyond retry-loop and spec-violation detection.
- HTML report generation.
- Phoenix, OpenTelemetry, or direct API ingestion.
- Baseline/diff workflows and CI integration.

## Requirements

- Python 3.11 or newer.
- [`uv`](https://docs.astral.sh/uv/) for dependency management and command execution.

## Quickstart

Run the built-in Langfuse fixture through the current end-to-end workflow:

```bash
uv sync
uv run agentprof init
uv run agentprof doctor
AGENTPROF_HASH_SALT=dev-salt uv run agentprof import langfuse-export \
  --observations tests/fixtures/langfuse_observations.json
uv run agentprof normalize
uv run agentprof analyze retry-loops
uv run agentprof analyze spec-violations
uv run agentprof cost ledger
uv run agentprof report generate
uv run agentprof report list
uv run agentprof store stats
```

The fixture does not include cost fields, so `agentprof cost ledger` will produce zero ledger entries for that sample. Real Langfuse exports with `totalCost` or `costDetails.total` values will populate `cost_ledger`.

## Typical Workflow

1. Initialize the workspace.

```bash
uv run agentprof init
```

This creates `agentprof.yml`, local workspace directories under `.agentprof/`, and the DuckDB store at `.agentprof/data/agentprof.duckdb`.

2. Configure privacy.

By default, `agentprof.yml` sets `privacy.hash_inputs: true`, so imports that contain input/output values require a salt in the environment variable named by `privacy.hmac_salt_env`.

```bash
export AGENTPROF_HASH_SALT='replace-with-a-stable-secret-for-this-project'
```

Use a stable per-project salt if you want hashes to remain comparable across repeated imports. Do not commit the salt.

3. Import a Langfuse observations export.

```bash
uv run agentprof import langfuse-export --observations path/to/observations.json
```

CSV exports are supported by file extension or explicit format:

```bash
uv run agentprof import langfuse-export \
  --observations path/to/observations.csv \
  --format csv
```

4. Normalize imported spans.

```bash
uv run agentprof normalize
```

This maps provider-specific observation payloads into canonical `normalized_spans` and `normalized_traces` tables.

5. Detect retry loops.

```bash
uv run agentprof analyze retry-loops
```

This writes `retry_loop` issues, issue evidence, and wasted retry costs when repeated failed attempts have the same trace, parent span, name, input retry fingerprint, and error signature.

6. Detect configured spec violations.

```bash
uv run agentprof analyze spec-violations
```

This writes `spec_violation` issues, issue evidence, and wasted spec-violation costs when spans violate required input or output fields configured in `agentprof.yml`.

7. Build the cost ledger.

```bash
uv run agentprof cost ledger
```

This replaces the current normalized-span cost ledger entries idempotently and prints a waterfall grouped by span status.

8. Generate a local report.

```bash
uv run agentprof report generate
```

This writes Markdown and JSON report files under `.agentprof/reports/` and stores report metadata in the `reports` table.

9. List or inspect generated reports.

```bash
uv run agentprof report list
uv run agentprof report show latest
uv run agentprof report show latest --format json
```

Use a stable `--report-id` when generating reports if you want a predictable ID such as `latest`.

10. Inspect store row counts.

```bash
uv run agentprof store stats
```

## CLI Commands

| Command | Purpose |
| --- | --- |
| `agentprof --help` | Show top-level CLI help. |
| `agentprof init` | Create `agentprof.yml`, workspace directories, and the DuckDB schema. |
| `agentprof doctor` | Validate that the local workspace and store are usable. |
| `agentprof import langfuse-export` | Import Langfuse observation exports into `raw_spans`. |
| `agentprof normalize` | Normalize raw imported spans into canonical trace/span tables. |
| `agentprof analyze retry-loops` | Detect repeated failing calls with the same retry fingerprint. |
| `agentprof analyze spec-violations` | Detect spans that violate configured required field contracts. |
| `agentprof cost ledger` | Build `cost_ledger` from normalized span costs and print a waterfall. |
| `agentprof report generate` | Generate Markdown and JSON reports from persisted analysis results. |
| `agentprof report list` | List generated reports recorded in the local store. |
| `agentprof report show REPORT_ID` | Print a generated report's Markdown or JSON artifact. |
| `agentprof store stats` | Show row counts for all store tables. |
| `agentprof store reset --yes` | Delete and recreate the local DuckDB store. |

## Spec Contracts

Spec-violation analysis is opt-in. Add contracts under `analyzers.spec_violations.contracts`:

```yaml
analyzers:
  spec_violations:
    contracts:
      - name: refund_policy_lookup
        required_input_fields:
          - customer_id
          - region
        required_output_fields:
          - answer
          - confidence
```

By default, a contract matches spans whose normalized `name` equals the contract `name`. Use `span_name` when you want a stable contract name that differs from the observed span name.

## Input Data

The current importer expects a Langfuse observations export in one of these shapes:

- A JSON array of observation objects.
- A JSON object with a `data` array.
- A JSON object with an `observations` array.
- A CSV file readable as observation rows.

Observation IDs, trace IDs, parent observation IDs, timestamps, status fields, model/provider fields, token usage, cost details, metadata, and sanitized privacy metadata are used during normalization where available.

## Privacy Model

AgentProf is designed to be local-first and privacy-conscious by default.

- No outbound telemetry is sent by AgentProf.
- Data is stored locally in DuckDB under `.agentprof/` unless `store.path` is changed.
- Raw input/output fields are not persisted by default.
- Common sensitive values are redacted before payload persistence.
- Supported redactions include emails, phone numbers, API keys/secrets, credit cards, JWTs, URLs with query strings, and sensitive mapping keys such as `authorization` and `api_key`.
- Input/output hashes use HMAC-SHA256 with your configured salt environment variable.
- Redacted evidence previews are capped by `privacy.max_evidence_chars`.

The default privacy config generated by `agentprof init` is:

```yaml
privacy:
  store_raw_io: false
  store_redacted_io: true
  hash_inputs: true
  hmac_salt_env: AGENTPROF_HASH_SALT
  max_evidence_chars: 500
```

## Reports

Generate reports after running analyzers and cost attribution:

```bash
uv run agentprof report generate
```

Use `--report-id` for a stable filename, or `--output-dir` to write files somewhere other than `.agentprof/reports/`:

```bash
uv run agentprof report generate \
  --report-id latest \
  --output-dir .agentprof/reports
```

Report JSON contains a machine-readable summary, issue details with evidence, and cost ledger entries. Report Markdown contains the same information in a local-first shareable format.

List and inspect generated reports:

```bash
uv run agentprof report list
uv run agentprof report show latest
uv run agentprof report show latest --format json
```

## Local Store

The DuckDB store currently includes these tables:

- `raw_spans`
- `raw_traces`
- `normalized_spans`
- `normalized_traces`
- `issues`
- `issue_evidence`
- `cost_ledger`
- `reports`

The `reports` table stores generated report metadata and points to the local Markdown/JSON output files.

## Development

Install dependencies and run tests:

```bash
uv sync
uv run pytest
```

Build package artifacts:

```bash
uv build
```

Useful local smoke workflow:

```bash
uv run agentprof store reset --yes
AGENTPROF_HASH_SALT=dev-salt uv run agentprof import langfuse-export \
  --observations tests/fixtures/langfuse_observations.json
uv run agentprof normalize
uv run agentprof analyze retry-loops
uv run agentprof analyze spec-violations
uv run agentprof cost ledger
uv run agentprof report generate
uv run agentprof report list
uv run agentprof store stats
```
