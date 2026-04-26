# AgentProf

Local-first CLI for AI-agent failure-and-waste reports.

AgentProf will import traces from systems such as Langfuse, normalize them into a local schema, run deterministic analyzers, and emit shareable reports that explain what failed, what wasted money, and what to fix.

This repository is in early MVP development.

## Quickstart

```bash
uv sync
uv run agentprof --help
uv run agentprof init
uv run agentprof doctor
uv run agentprof store stats
```

## Development

```bash
uv run pytest
```
