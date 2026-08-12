# Demo Reports

These artifacts are generated from AgentProf's bundled synthetic demo traces. They contain no customer data.

The demo shows how AgentProf can debug Langfuse traces, detect agent retry loops, and act as a local LLM agent cost profiler without uploading traces to a hosted observability service.

Use these files when you want to show the problem before explaining the tool:

- `trace-demo-retry` repeats the same failing `refund_policy_lookup` call and wastes $0.006000000 on the retry.
- The same tool call violates the configured contract because `region` is missing.
- `trace-multi-agent-1` costs $0.084000000 across 3 agents; AgentProf estimates $0.042000000 of orchestration overhead against a $0.042000000 single-agent baseline.
- The full demo report totals 4 issues, $0.054000000 of deduplicated estimated wasted spend, and $0.006000000 of overlapping attribution.

Refresh them with:

```bash
uv run agentprof demo --dir examples/generated-demo
cp examples/generated-demo/reports/demo.* examples/reports/
```

Files:

- `demo.html`
- `demo.md`
- `demo.json`
- `demo-multi-agent-waste.svg`

Related guide:

- [Debug Langfuse traces with AgentProf](../../docs/debug-langfuse-traces.md)
