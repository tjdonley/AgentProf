# Contributing

Thanks for helping make AgentProf sharper. The project is intentionally local-first and analyzer-driven: prefer deterministic checks, clear evidence, and small fixtures over broad magic.

## Development Setup

```bash
uv sync
uv run pytest
uv run agentprof demo
```

If `uv` is unavailable, use Python 3.11 or newer and install the package with its test dependencies in a virtual environment.

## Good Contribution Paths

- **Analyzer ideas:** add a deterministic issue detector that reads normalized spans/traces and writes `issues`, `issue_evidence`, and analyzer-attributed `cost_ledger` rows.
- **Import sources:** add a safe importer that preserves privacy defaults and maps into the existing normalization path.
- **Report polish:** improve static Markdown/JSON/HTML reports without adding external network dependencies.
- **Fixtures:** add small synthetic traces that demonstrate one failure or waste pattern clearly.

## Analyzer Checklist

- Keep the finding explainable from stored evidence.
- Make reruns idempotent by replacing only the analyzer's own issue kind and cost attribution method.
- Avoid double-counting parent/child costs.
- Add tests for empty input, stale-result clearing, idempotency, and CLI output.
- Add or update a fixture if the finding is part of the public demo story.

## Privacy Rules

- Do not store raw input/output by default.
- Redact sensitive values before persistence.
- Prefer HMAC hashes or redacted previews for matching and evidence.
- Do not add network calls to report generation or local analysis.

## Pull Requests

Before opening a PR:

```bash
uv run pytest
uv run agentprof demo
uv build
```

In the PR description, include what changed, why it matters to users, and what validation you ran.
