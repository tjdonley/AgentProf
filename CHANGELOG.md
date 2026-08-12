# Changelog

All notable changes to AgentProf will be documented here.

## 0.1.0 - Unreleased

### Added

- Token-based cost estimation for spans that arrive without provider cost fields, with a configurable `pricing` model rate table and a bundled default table.
- `agentprof pricing list` to show the effective model rates and where each came from.
- Estimated-versus-provider cost reporting in `agentprof normalize`, `agentprof cost ledger`, and report ledger tables.
- `Top Findings` section in Markdown and HTML reports, plus `top_findings` and `summary.top_issue_kind` in the JSON payload.
- One-command `agentprof demo` workflow with bundled sample traces.
- Static HTML report generation alongside Markdown and JSON reports.
- Multi-agent waste SVG visual artifacts.
- MIT license and package metadata for public distribution.
- Project pop spec, contribution guide, security policy, and GitHub issue templates.

### Changed

- Report issues are ranked by attributed waste, then severity and confidence, instead of by issue ID.
- Retry-loop severity now escalates with repeated wasted attempts, and multi-agent severity tracks the cost multiple over baseline, instead of being fixed.
- README now leads with concrete trace-waste findings and the demo workflow.
- CI now tests Python 3.11 and 3.12, runs demo smoke checks, builds the package, and validates wheel installation.
