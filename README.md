# AgentProf

Local-first CLI for AI-agent failure-and-waste reports.

AgentProf will import traces from systems such as Langfuse, normalize them into a local schema, run deterministic analyzers, and emit shareable reports that explain what failed, what wasted money, and what to fix.

This repository is in early MVP development.

By default, AgentProf is designed to hash inputs, redact common sensitive values, and cap evidence snippets before trace data is persisted locally.

## Quickstart

```bash
uv sync
uv run agentprof --help
uv run agentprof init
uv run agentprof doctor
uv run agentprof store stats
AGENTPROF_HASH_SALT=dev-salt uv run agentprof import langfuse-export \
  --observations tests/fixtures/langfuse_observations.json
```

## Development

```bash
uv run pytest
```
