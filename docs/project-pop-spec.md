# Project Pop Spec

This spec turns AgentProf from a capable local trace tool into a project people can understand, try, screenshot, and recommend quickly.

The goal is not to become a full observability platform. The goal is to own a sharper category:

> AgentProf is the local profiler for AI-agent traces. It finds retry loops, contract failures, and wasted orchestration spend in traces you already have.

## Problem

Teams are building agent systems that look impressive in demos but become hard to debug in production. Existing observability products help collect and inspect traces, but they often stop at showing what happened. AgentProf should answer the next question:

> What did this agent waste, why did it happen, and what should I fix first?

The current project has a strong technical base: Langfuse import, privacy controls, DuckDB storage, deterministic analyzers, cost attribution, and Markdown/JSON/HTML reports. What it is missing is a faster path to the "I get it" moment and a public surface that makes the output feel worth sharing.

## Product Thesis

AgentProf should feel like `pytest` or `cargo clippy` for agent traces:

- Run it locally.
- Point it at trace data.
- Get specific findings.
- Keep private data on your machine.
- Use the result to fix wasted agent behavior.

The product should not lead with tables, schemas, or storage. It should lead with findings:

- "This tool call failed 4 times with the same input."
- "This trace spent 50 percent of its cost on multi-agent orchestration overhead."
- "This span violated the configured contract before the agent continued."
- "This run could have saved $0.042 with a simpler single-agent path."

## Audience

Primary users:

- Engineers building agents with LangGraph, CrewAI, AutoGen, custom orchestrators, or Langfuse-backed workflows.
- Founders and staff engineers trying to reduce LLM spend without adding another hosted platform.
- AI platform teams that need local, auditable trace postmortems.

Secondary users:

- Consultants debugging customer agent systems.
- Researchers comparing single-agent and multi-agent workflows.
- Open-source maintainers who want reproducible agent failure fixtures.

Non-users for now:

- Teams that need hosted real-time dashboards.
- Teams that do not have trace exports.
- Teams looking for prompt management, eval datasets, or production tracing collection.

## Positioning

Short tagline:

> Find hidden waste in your AI-agent traces.

One-sentence pitch:

> AgentProf is a local CLI that turns existing agent traces into evidence-backed reports on retries, contract failures, and wasted orchestration cost.

Comparison frame:

| Tool category | What it does | AgentProf relationship |
| --- | --- | --- |
| Langfuse, Phoenix, LangSmith | Collects and explores LLM traces | AgentProf imports trace data and performs offline waste analysis. |
| OpenTelemetry / OpenInference | Standardizes instrumentation and trace shape | AgentProf should consume these formats as import sources. |
| Evals frameworks | Measures quality on test sets | AgentProf explains waste and failure patterns inside traces. |
| Cost dashboards | Summarize spend | AgentProf attributes spend to fixable behavior. |

## North Star Experience

A new visitor should be able to understand the project in one minute and try it in five.

Target flow:

```bash
uvx agentprof demo --open
```

Expected outcome:

1. A self-contained demo workspace is created.
2. Bundled multi-agent Langfuse sample traces are imported.
3. AgentProf normalizes, analyzes, builds cost attribution, and generates reports.
4. The HTML report opens locally.
5. The report headline shows concrete waste and a top fix recommendation.

The user should not need to understand DuckDB, salts, configs, or analyzers before seeing a report.

## Success Metrics

Launch metrics:

- 100 GitHub stars within 30 days of a polished v0.1 launch.
- 10 external users run the demo and give feedback.
- 3 external issues opened by users who tried their own trace data.
- 1 public writeup or demo video showing a real finding.

Product metrics:

- Time from install to visible demo report: under 90 seconds.
- Time from Langfuse export to report: under 5 minutes.
- At least 3 issue kinds can produce actionable findings from bundled fixtures.
- README first viewport explains what AgentProf finds without scrolling.

Quality metrics:

- Demo command works from a package install, not only from repo checkout.
- Tests cover bundled resources, demo output, report paths, and HTML escaping.
- No raw input/output is stored by default.

## Workstream 1: One-Command Demo

Status: started. The repository already contains a `demo` command path and bundled demo data under `agentprof/demo/`.

User promise:

> I can run one command and immediately see the kind of waste AgentProf finds.

Functional requirements:

- Add or finish `agentprof demo`.
- Use bundled sample traces.
- Create a self-contained workspace outside `.agentprof` by default, such as `agentprof-demo/`.
- Set a demo-only salt automatically if the user has not configured one.
- Generate Markdown, JSON, HTML, and optional SVG artifacts.
- Print a short result summary:
  - issues found
  - estimated wasted spend
  - top finding
  - report paths
- Support `--open` to open the HTML report.

Acceptance criteria:

- `uv run agentprof demo` succeeds from a clean checkout.
- `uv run agentprof demo --open` opens the generated HTML report.
- The command does not require `agentprof init`.
- The command does not modify the user's real `.agentprof` workspace.
- Packaged demo data is included in the built distribution.
- README quickstart leads with the demo before the full import workflow.

Implementation notes:

- Keep the demo pipeline explicit in `agentprof/cli.py` or move orchestration into `agentprof/demo/runner.py` if it grows.
- Use `importlib.resources` for packaged sample data.
- Add tests for CLI output, generated artifact existence, and package data inclusion.

## Workstream 2: Screenshotable Report

User promise:

> The report makes the problem obvious enough to share with a teammate.

The current HTML report is useful, but the next version should optimize for visual clarity and memorability.

Design goals:

- Put the main finding above the fold.
- Make wasted cost and potential savings visually dominant.
- Show issue cards as "money leak" or "failure pattern" cards, not just records.
- Keep evidence inspectable but secondary.
- Keep the report fully static and local.
- Preserve HTML escaping and sibling-only artifact safety.

Required report sections:

- Header:
  - project name
  - generated timestamp
  - report ID
  - short privacy/local note
- Executive summary:
  - total issues
  - total estimated wasted spend
  - potential savings
  - affected traces
  - top issue kind
- Top findings:
  - top 3 issues by wasted cost, severity, or confidence
  - each includes recommendation and one evidence line
- Visuals:
  - multi-agent waste chart when available
  - future analyzer visuals when available
- Evidence:
  - collapsible details for span IDs, trace IDs, attributes, and previews
- Cost ledger:
  - compact table
  - group by attribution method or cost type

Acceptance criteria:

- Demo report has an immediately visible "estimated wasted spend" summary.
- A screenshot of the report communicates what AgentProf does without reading the README.
- HTML report still renders without external CSS, JS, fonts, or network requests.
- Existing report escaping tests continue to pass.

## Workstream 3: README That Converts

User promise:

> I understand what AgentProf does before I read the architecture.

The README should be rewritten around the outcome first and the pipeline second.

Recommended first viewport:

1. Logo.
2. Tagline: "Find hidden waste in your AI-agent traces."
3. One sentence pitch.
4. Screenshot or GIF of the HTML report.
5. Three bullets:
   - Detect repeated failed calls.
   - Estimate multi-agent orchestration overhead.
   - Keep trace analysis local.
6. Install/demo command.

Suggested opening copy:

```markdown
# AgentProf

Find hidden waste in your AI-agent traces.

AgentProf is a local CLI that turns Langfuse exports and other trace data into evidence-backed reports on retry loops, contract failures, and wasted orchestration cost. It runs on your machine, stores data in DuckDB, and writes static reports you can inspect or share internally.
```

README structure:

- What AgentProf finds
- Demo in one command
- Screenshot / sample report
- Why local-first
- Supported imports
- Current analyzers
- Typical workflow
- Report examples
- Roadmap
- Contributing

Acceptance criteria:

- The full manual workflow is still documented, but not first.
- The README includes a screenshot or generated report image near the top.
- The README has a "What AgentProf is not" section to avoid dashboard confusion.
- The README links to a sample HTML report artifact or checked-in screenshot.

## Workstream 4: Public Example Assets

User promise:

> I can inspect the output before installing anything.

Deliverables:

- `examples/reports/demo.html`
- `examples/reports/demo.json`
- `examples/reports/demo.md`
- `examples/reports/demo-multi-agent-waste.svg`
- `assets/demo-report-screenshot.png`

Acceptance criteria:

- Example artifacts are generated from bundled demo data.
- README links to the example report and embeds the screenshot.
- The example report contains no private or raw customer data.
- A maintainer command can refresh examples deterministically.

Possible command:

```bash
uv run agentprof demo --dir examples/generated-demo
```

Then copy stable artifacts into `examples/reports/` and update the screenshot.

## Workstream 5: Analyzer Expansion

User promise:

> AgentProf finds more than one kind of obvious waste.

Near-term analyzers should be deterministic, explainable, and testable from normalized spans. Avoid LLM-based diagnosis for now.

Candidate analyzers:

| Analyzer | Finding | Required data | Priority |
| --- | --- | --- | --- |
| Dead-end tool error | Tool failed, agent continued, final trace failed | status, parent links, outcome | P1 |
| Unused retrieval | Retriever or search cost incurred, no downstream use signal | span type, child order, output preview/hash | P1 |
| Excessive handoffs | Agent count or handoff count exceeds threshold | span type, agent name | P1 |
| Model overkill | Expensive model used for low-risk helper span | model, cost, span type/name | P2 |
| Planner chatter | Planning spans consume high cost before first tool/result | span order, model spans, cost | P2 |
| Long no-output trace | High cost with empty or failed final output | trace outcome, output hash/preview, cost | P2 |

First new analyzer recommendation:

> `dead_end_tool_error`

Why:

- Easy to explain.
- Easy to test.
- Common in agent systems.
- Does not require complex semantic analysis.
- Produces clear advice: stop, repair input, or branch on tool failure.

Acceptance criteria:

- Analyzer writes `issues`, `issue_evidence`, and cost records idempotently.
- Analyzer has CLI command under `agentprof analyze`.
- Analyzer is represented in the demo fixture or a new fixture.
- Report shows it as a top finding when present.

## Workstream 6: Import Compatibility

User promise:

> I do not need to use exactly one tracing vendor to try AgentProf.

Current support:

- Langfuse observation export JSON and CSV.

Recommended next imports:

1. Generic AgentProf JSONL.
2. OpenInference / OpenTelemetry JSON export.
3. Phoenix export if the format is straightforward.

The generic format should come first because it gives users and contributors a stable target.

Example generic JSONL shape:

```json
{"trace_id":"t1","span_id":"s1","parent_span_id":null,"name":"root","span_type":"root","status":"ok","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T00:00:01Z","cost_usd":"0.01"}
```

Acceptance criteria:

- `agentprof import jsonl --spans path/to/spans.jsonl` works without Langfuse-specific fields.
- Import docs include a minimal example.
- Normalization remains source-aware and does not weaken privacy defaults.
- Fixtures cover at least one non-Langfuse trace.

## Workstream 7: CI And Budget Gates

User promise:

> AgentProf can prevent known waste from creeping back in.

Add a command that turns reports into pass/fail checks.

Possible command:

```bash
agentprof budget check \
  --max-wasted-cost 1.00 \
  --max-medium-issues 0 \
  --max-high-issues 0
```

Functional requirements:

- Read persisted issues and cost ledger.
- Exit non-zero when thresholds are exceeded.
- Print the failing issue IDs and recommendations.
- Support JSON output for CI annotations later.

Acceptance criteria:

- Command exits `0` when thresholds pass.
- Command exits `1` when thresholds fail.
- README includes a GitHub Actions example.

## Workstream 8: Packaging And Release Polish

User promise:

> The project looks alive, safe to try, and easy to install.

Required launch checklist:

- License file present.
- PyPI package published.
- `uvx agentprof demo` works.
- GitHub release `v0.1.0` exists.
- CI badge in README.
- Screenshot in README.
- GitHub topics set:
  - `ai-agents`
  - `llm`
  - `llmops`
  - `agentops`
  - `observability`
  - `langfuse`
  - `duckdb`
  - `opentelemetry`
  - `openinference`
  - `cost-optimization`
- Issue templates:
  - bug report
  - analyzer idea
  - import source request
- `CONTRIBUTING.md`
- `SECURITY.md`
- `CHANGELOG.md`

Acceptance criteria:

- A new user can install without cloning.
- The repo communicates current status without sounding abandoned or apologetic.
- The roadmap invites contributors into scoped work.

## Workstream 9: Launch Narrative

User promise:

> This is not another dashboard. It is a practical profiler for agent waste.

Core launch story:

> We keep adding observability to agent systems, but many traces still require manual inspection. AgentProf runs locally on trace exports and produces deterministic reports that point to fixable waste: repeated failed calls, contract violations, and multi-agent overhead.

Launch title ideas:

- "Show HN: AgentProf - find hidden waste in AI-agent traces"
- "I built a local profiler for AI-agent trace waste"
- "AgentProf: evidence-backed failure and waste reports for AI agents"

Demo story:

1. Multi-agent trace costs $0.084.
2. Estimated single-agent baseline is $0.042.
3. AgentProf flags $0.042 of estimated orchestration overhead.
4. The report shows the affected trace, agent count, cost multiple, and recommendation.

Launch channels:

- GitHub release.
- Hacker News "Show HN".
- Langfuse community.
- LangGraph / LangChain community spaces.
- OpenTelemetry / OpenInference-adjacent discussions.
- AI engineering Discords and Slack groups.
- A short screencast showing `agentprof demo --open`.

Acceptance criteria:

- Launch post links directly to the demo command and screenshot.
- Launch post includes one concrete finding, not just project features.
- README is ready before external posting starts.

## Roadmap

### v0.1: Make It Tryable

- Finish `agentprof demo`.
- Include packaged demo data.
- Add screenshotable report improvements.
- Rewrite README first viewport.
- Add public example report assets.
- Publish PyPI package.
- Cut first GitHub release.

### v0.2: Make It Useful On More Traces

- Add generic JSONL import.
- Add one new analyzer: `dead_end_tool_error`.
- Add budget gate command.
- Add richer report top-findings section.

### v0.3: Make It Ecosystem-Friendly

- Add OpenInference/OpenTelemetry import.
- Add observed baseline workflows to docs.
- Add contributor guide for analyzers.
- Add fixture generator or anonymization helper.

### v0.4: Make It Team-Friendly

- Add report diffing.
- Add CI examples.
- Add machine-readable summary output.
- Add SARIF or GitHub annotation output if useful.

## Non-Goals

- No hosted dashboard.
- No trace collection server.
- No JavaScript app framework for reports.
- No external network dependencies for report rendering.
- No LLM-based automatic diagnosis until deterministic analyzers are strong.
- No weakening privacy defaults to make demos easier.

## Risks

Risk: Users compare AgentProf to full observability platforms.

Mitigation: Position as a local profiler and postmortem tool that complements trace platforms.

Risk: Demo looks synthetic.

Mitigation: Make the fixture realistic and invite users to submit sanitized exports for better examples.

Risk: Reports become visually busy.

Mitigation: Optimize for top findings first, with evidence behind details.

Risk: Analyzer claims feel too speculative.

Mitigation: Label estimates clearly and prefer deterministic findings with evidence.

Risk: Import support limits adoption.

Mitigation: Add generic JSONL early so any trace producer can target AgentProf.

## Immediate Next Tasks

1. Finish and test `agentprof demo` end to end.
2. Ensure demo package data is included in builds.
3. Generate a demo HTML report and screenshot.
4. Rewrite the README top section around the demo.
5. Add `examples/reports/` artifacts.
6. Add `CONTRIBUTING.md` with analyzer and import-source contribution paths.
7. Add issue templates for analyzer ideas and import requests.
8. Publish `v0.1.0` once the demo command works from an installed package.

## Definition Of "Pops"

AgentProf pops when a new visitor can say this after one minute:

> "Oh, this finds where my agent wasted money and gives me a local report I can act on."

Everything in this plan should serve that moment.
