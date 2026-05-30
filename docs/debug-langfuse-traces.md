# Debug Langfuse Traces With AgentProf

AgentProf is built for the moment after a trace viewer shows what happened and you need to know what was wasted, why it happened, and what to fix first.

This guide uses the bundled synthetic demo traces. They are small on purpose: the report should make the failure pattern obvious before you connect private production exports.

## Start With The Broken Trace

The demo includes two trace stories:

| Trace | Before AgentProf | After AgentProf |
| --- | --- | --- |
| `trace-demo-retry` | A support agent calls `refund_policy_lookup` twice and both calls fail with `missing required field region`. | AgentProf emits a `retry_loop` and two `spec_violation` findings with affected span IDs, evidence, and recommended tests. |
| `trace-multi-agent-1` | A triage agent delegates to `research_agent` and `policy_agent`, producing a correct answer but extra orchestration cost. | AgentProf emits `multi_agent_waste`: actual cost $0.084000000, estimated single-agent baseline $0.042000000, estimated overhead $0.042000000, cost multiple 2.00x. |

Run it:

```bash
uv run agentprof demo --open
```

Or inspect the checked-in artifacts:

- [Demo HTML report](../examples/reports/demo.html)
- [Demo Markdown report](../examples/reports/demo.md)
- [Demo JSON payload](../examples/reports/demo.json)

## Detect Agent Retry Loops

Trace viewers can show repeated calls. AgentProf turns those repetitions into a fixable finding when the same failing call repeats with the same retry fingerprint and error signature.

From the demo report:

```text
Repeated failing call to refund_policy_lookup
Wasted cost: $0.006000000
Evidence:
- trace-demo-retry:demo-tool-retry-1 Attempt 1 failed with missing required field region.
- trace-demo-retry:demo-tool-retry-2 Attempt 2 failed with missing required field region.
Recommendation: Stop retrying deterministic failures until the input, schema, or tool precondition has changed.
```

That is the core AgentProf posture: not just "there were two calls," but "the second call was waste unless the agent changed the input or precondition."

## Find Broken Tool Contracts

The same trace also violates a configured tool contract because `refund_policy_lookup` requires `customer_id` and `region`, but the demo input only includes `customer_id`.

```text
Contract violation in refund_policy_lookup
Evidence:
- trace-demo-retry:demo-tool-retry-2 refund_policy_lookup violated refund_policy_lookup: missing input fields: region.
Recommended test: Add a contract test for refund_policy_lookup covering required fields.
```

This is useful when a Langfuse trace tells you a tool failed, but the fix is really upstream validation: do not continue an agent path until required fields exist.

## Use It As An LLM Agent Cost Profiler

AgentProf also profiles cost that came from orchestration shape, not only failed calls. In the demo, a three-agent trace costs twice the configured single-agent baseline estimate:

| Metric | Value |
| --- | ---: |
| Actual multi-agent trace cost | $0.084000000 |
| Estimated single-agent baseline | $0.042000000 |
| Estimated orchestration overhead | $0.042000000 |
| Cost multiple | 2.00x |
| Agents detected | 3 |

This does not prove the multi-agent path is wrong. It gives you a concrete comparison to validate with your own single-agent baseline traces.

## Local Agent Observability Workflow

Use AgentProf when you already have Langfuse observation exports and want local agent observability without shipping traces to another service:

```bash
uv run agentprof init
export AGENTPROF_HASH_SALT='dev-salt-value-at-least-16-bytes'
uv run agentprof import langfuse-export --observations path/to/observations.json
uv run agentprof normalize
uv run agentprof analyze retry-loops
uv run agentprof analyze spec-violations
uv run agentprof analyze multi-agent-waste
uv run agentprof cost ledger
uv run agentprof report generate --report-id latest
uv run agentprof report show latest --format html
```

The output is a static local report with issue summaries, evidence, recommendations, cost attribution, and optional SVG visuals.
