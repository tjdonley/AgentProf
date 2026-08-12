from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from typer.testing import CliRunner

from agentprof.cli import app
from agentprof.config import (
    AgentProfConfig,
    ModelPriceConfig,
    PricingConfig,
    load_config,
)
from agentprof.cost.pricing import (
    apply_estimated_costs,
    build_pricing_table,
    estimate_span_cost,
)
from agentprof.cost.runner import build_cost_ledger
from agentprof.normalize.runner import build_normalized_traces, normalize_store
from agentprof.normalize.schema import NormalizedSpan
from agentprof.store.duckdb_store import DuckDBStore


runner = CliRunner()
FIXTURES = Path(__file__).parent / "fixtures"


def test_default_table_prices_a_known_model() -> None:
    table = build_pricing_table(PricingConfig())

    price = table.lookup("gpt-4o-mini")

    assert price is not None
    assert price.source == "default"
    assert price.input_per_1m_usd == Decimal("0.15")
    assert price.output_per_1m_usd == Decimal("0.60")


def test_lookup_prefers_the_longest_matching_prefix() -> None:
    table = build_pricing_table(PricingConfig())

    dated = table.lookup("gpt-4o-mini-2024-07-18")

    assert dated is not None
    assert dated.model == "gpt-4o-mini"


def test_lookup_respects_version_boundaries() -> None:
    table = build_pricing_table(
        PricingConfig(
            use_default_table=False,
            models=[
                ModelPriceConfig(
                    model="gpt-4",
                    input_per_1m_usd=Decimal("30"),
                    output_per_1m_usd=Decimal("60"),
                )
            ],
        )
    )

    # `gpt-4o` is a different model and must not inherit the `gpt-4` rate.
    assert table.lookup("gpt-4o") is None
    assert table.lookup("gpt-4-turbo") is not None
    assert table.lookup("gpt-4") is not None


def test_lookup_strips_a_provider_prefix() -> None:
    table = build_pricing_table(PricingConfig())

    price = table.lookup("anthropic/claude-sonnet-4-5")

    assert price is not None
    assert price.model == "claude-sonnet-4-5"


def test_configured_prices_override_the_default_table() -> None:
    table = build_pricing_table(
        PricingConfig(
            models=[
                ModelPriceConfig(
                    model="gpt-4o-mini",
                    input_per_1m_usd=Decimal("1"),
                    output_per_1m_usd=Decimal("2"),
                )
            ]
        )
    )

    price = table.lookup("gpt-4o-mini")

    assert price is not None
    assert price.source == "config"
    assert price.input_per_1m_usd == Decimal("1")


def test_disabled_pricing_builds_an_empty_table() -> None:
    table = build_pricing_table(PricingConfig(enabled=False))

    assert not table
    assert table.lookup("gpt-4o-mini") is None


def test_estimate_span_cost_uses_token_counts() -> None:
    table = build_pricing_table(PricingConfig())

    estimate = estimate_span_cost(_span(input_tokens=1000, output_tokens=500), table)

    assert estimate is not None
    # 1000 * 0.15/1M + 500 * 0.60/1M
    assert estimate.cost_usd == Decimal("0.000450000")


def test_estimate_span_cost_never_overwrites_a_provider_cost() -> None:
    table = build_pricing_table(PricingConfig())
    span = _span(input_tokens=1000, output_tokens=500, cost_usd=Decimal("9"))

    assert estimate_span_cost(span, table) is None


def test_estimate_span_cost_skips_spans_without_usable_tokens() -> None:
    table = build_pricing_table(PricingConfig())

    assert estimate_span_cost(_span(), table) is None
    assert estimate_span_cost(_span(input_tokens=0, output_tokens=0), table) is None


def test_estimate_span_cost_skips_unpriced_models() -> None:
    table = build_pricing_table(PricingConfig())
    span = _span(model_name="internal-router-v2", input_tokens=100, output_tokens=50)

    assert estimate_span_cost(span, table) is None


def test_apply_estimated_costs_marks_confidence_and_reports_gaps() -> None:
    table = build_pricing_table(PricingConfig())
    spans = [
        _span(span_id="priced", input_tokens=1000, output_tokens=500),
        _span(
            span_id="unpriced",
            model_name="internal-router-v2",
            input_tokens=100,
            output_tokens=50,
        ),
        _span(span_id="already-costed", input_tokens=10, cost_usd=Decimal("0.5")),
    ]

    updated, result = apply_estimated_costs(spans, table)
    by_id = {span.span_id: span for span in updated}

    assert result.estimated_spans == 1
    assert result.estimated_cost_usd == Decimal("0.000450000")
    assert result.unpriced_models == ("internal-router-v2",)
    assert by_id["priced"].cost_confidence == "estimated"
    assert by_id["priced"].cost_usd == Decimal("0.000450000")
    assert by_id["unpriced"].cost_usd is None
    assert by_id["unpriced"].cost_confidence == "unknown"
    assert by_id["already-costed"].cost_usd == Decimal("0.5")
    assert by_id["already-costed"].cost_confidence == "source"


def test_normalize_estimates_cost_for_token_only_export(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    _import_token_only_fixture(store)

    result = normalize_store(store, pricing=PricingConfig())
    ledger = build_cost_ledger(store)

    # 0.000450000 (gpt-4o-mini) + 0.021000000 (claude-sonnet-4-5)
    assert result.estimated_cost_spans == 2
    assert result.estimated_cost_usd == Decimal("0.021450000")
    assert result.unpriced_models == ["internal-router-v2"]
    assert result.data_quality.spans_with_estimated_cost == 2
    assert result.data_quality.spans_with_source_cost == 0
    assert ledger.ledger_entries == 2
    assert ledger.total_cost_usd == Decimal("0.021450000")
    assert ledger.estimated_cost_usd == Decimal("0.021450000")
    assert ledger.source_cost_usd == Decimal("0")


def test_normalize_leaves_costs_untouched_when_pricing_is_disabled(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    _import_token_only_fixture(store)

    result = normalize_store(store, pricing=PricingConfig(enabled=False))
    ledger = build_cost_ledger(store)

    assert result.estimated_cost_spans == 0
    assert result.estimated_cost_usd == Decimal("0")
    assert ledger.ledger_entries == 0
    assert ledger.total_cost_usd == Decimal("0")


def test_normalize_is_idempotent_with_estimated_costs(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    _import_token_only_fixture(store)

    first = normalize_store(store, pricing=PricingConfig())
    second = normalize_store(store, pricing=PricingConfig())

    assert first == second
    assert store.stats()["normalized_spans"] == 4


def test_default_config_round_trips_pricing_defaults(tmp_path: Path) -> None:
    with runner.isolated_filesystem(temp_dir=tmp_path):
        assert runner.invoke(app, ["init"]).exit_code == 0
        config = load_config()

    assert config.pricing.enabled is True
    assert config.pricing.use_default_table is True
    assert config.pricing.models == []


def test_config_parses_model_prices_without_float_drift(tmp_path: Path) -> None:
    config_path = tmp_path / "agentprof.yml"
    config_path.write_text(
        "pricing:\n"
        "  models:\n"
        "    - model: house-model\n"
        "      input_per_1m_usd: 0.15\n"
        "      output_per_1m_usd: 0.60\n",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.pricing.models[0].input_per_1m_usd == Decimal("0.15")
    assert config.pricing.models[0].output_per_1m_usd == Decimal("0.60")


def test_cli_normalize_reports_estimated_cost_and_unpriced_models(monkeypatch) -> None:
    monkeypatch.setenv("AGENTPROF_HASH_SALT", "test-salt-value-123")
    with runner.isolated_filesystem():
        assert runner.invoke(app, ["init"]).exit_code == 0
        import_result = runner.invoke(
            app,
            [
                "import",
                "langfuse-export",
                "--observations",
                str(FIXTURES / "langfuse_token_only_observations.json"),
            ],
        )
        normalize_result = runner.invoke(app, ["normalize"])
        ledger_result = runner.invoke(app, ["cost", "ledger"])

    assert import_result.exit_code == 0
    assert normalize_result.exit_code == 0
    assert ledger_result.exit_code == 0
    assert "estimated cost for 2 span(s)" in normalize_result.output
    assert "No price configured for internal-router-v2" in normalize_result.output
    assert "$0.021450000" in ledger_result.output


def test_cli_pricing_list_shows_default_and_configured_rates() -> None:
    with runner.isolated_filesystem():
        assert runner.invoke(app, ["init"]).exit_code == 0
        Path("agentprof.yml").write_text(
            "pricing:\n"
            "  models:\n"
            "    - model: house-model\n"
            "      input_per_1m_usd: '1.00'\n"
            "      output_per_1m_usd: '2.00'\n",
            encoding="utf-8",
        )
        result = runner.invoke(app, ["pricing", "list"])

    assert result.exit_code == 0
    assert "house-model" in result.output
    assert "gpt-4o-mini" in result.output
    assert "Bundled default rates were recorded on" in result.output


def test_cli_pricing_list_reports_disabled_estimation() -> None:
    with runner.isolated_filesystem():
        assert runner.invoke(app, ["init"]).exit_code == 0
        Path("agentprof.yml").write_text(
            "pricing:\n  enabled: false\n", encoding="utf-8"
        )
        result = runner.invoke(app, ["pricing", "list"])

    assert result.exit_code == 0
    assert "Cost estimation is disabled" in result.output


def _import_token_only_fixture(store: DuckDBStore) -> None:
    from agentprof.ingest.langfuse_export import import_langfuse_export

    config = AgentProfConfig()
    config.privacy.hash_inputs = False
    import_langfuse_export(
        observations_path=FIXTURES / "langfuse_token_only_observations.json",
        store=store,
        config=config,
    )


def _span(
    *,
    span_id: str = "llm",
    parent_span_id: str | None = "root",
    model_name: str | None = "gpt-4o-mini",
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    cost_usd: Decimal | None = None,
) -> NormalizedSpan:
    return NormalizedSpan(
        trace_id="trace",
        span_id=span_id,
        parent_span_id=parent_span_id,
        source="langfuse",
        name="generate",
        span_type="llm",
        model_name=model_name,
        status="ok",
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_usd=cost_usd,
        cost_confidence="source" if cost_usd is not None else "unknown",
    )


def test_estimation_never_displaces_a_provider_costed_ancestor() -> None:
    table = build_pricing_table(PricingConfig())
    parent = _span(
        span_id="parent",
        parent_span_id=None,
        model_name=None,
        cost_usd=Decimal("1.00"),
    )
    child = _span(span_id="child", parent_span_id="parent", input_tokens=1000, output_tokens=500)

    updated, result = apply_estimated_costs([parent, child], table)
    trace = build_normalized_traces(updated)[0]

    # Cost attribution drops a costed ancestor once a descendant is costed, so
    # estimating the child here would replace $1.00 of provider spend with an
    # estimate two orders of magnitude smaller.
    assert result.estimated_spans == 0
    assert trace.total_cost_usd == Decimal("1.00")
    assert {span.span_id: span.cost_usd for span in updated} == {
        "parent": Decimal("1.00"),
        "child": None,
    }


def test_estimation_skips_spans_above_a_provider_costed_descendant() -> None:
    table = build_pricing_table(PricingConfig())
    parent = _span(span_id="parent", parent_span_id=None, input_tokens=1000, output_tokens=500)
    child = _span(
        span_id="child",
        parent_span_id="parent",
        model_name=None,
        cost_usd=Decimal("0.75"),
    )

    updated, result = apply_estimated_costs([parent, child], table)
    trace = build_normalized_traces(updated)[0]

    assert result.estimated_spans == 0
    assert trace.total_cost_usd == Decimal("0.75")


def test_estimation_still_prices_sibling_branches_without_provider_cost() -> None:
    table = build_pricing_table(PricingConfig())
    root = _span(span_id="root", parent_span_id=None, model_name=None)
    costed = _span(
        span_id="costed-branch",
        parent_span_id="root",
        model_name=None,
        cost_usd=Decimal("0.50"),
    )
    uncosted = _span(
        span_id="token-branch",
        parent_span_id="root",
        input_tokens=1000,
        output_tokens=500,
    )

    updated, result = apply_estimated_costs([root, costed, uncosted], table)
    by_id = {span.span_id: span for span in updated}

    # The root is blocked because it sits above provider spend, but an
    # uncosted sibling branch is still fair game.
    assert result.estimated_spans == 1
    assert by_id["token-branch"].cost_usd == Decimal("0.000450000")
    assert by_id["root"].cost_usd is None
    assert build_normalized_traces(updated)[0].total_cost_usd == Decimal("0.500450000")


def test_blocked_spans_are_not_reported_as_unpriced_models() -> None:
    table = build_pricing_table(PricingConfig())
    parent = _span(
        span_id="parent",
        parent_span_id=None,
        model_name=None,
        cost_usd=Decimal("1.00"),
    )
    child = _span(
        span_id="child",
        parent_span_id="parent",
        model_name="internal-router-v2",
        input_tokens=100,
        output_tokens=50,
    )

    _, result = apply_estimated_costs([parent, child], table)

    assert result.unpriced_models == ()


def test_effective_prices_drop_shadowed_defaults() -> None:
    table = build_pricing_table(
        PricingConfig(
            models=[
                ModelPriceConfig(
                    model="gpt-4o-mini",
                    input_per_1m_usd=Decimal("1"),
                    output_per_1m_usd=Decimal("2"),
                )
            ]
        )
    )

    effective = [price for price in table.effective_prices() if price.model == "gpt-4o-mini"]

    assert len(effective) == 1
    assert effective[0].source == "config"
    # A configured entry only shadows its exact name.
    assert any(price.model == "gpt-4o" for price in table.effective_prices())


def test_cli_pricing_list_shows_one_row_per_overridden_model() -> None:
    with runner.isolated_filesystem():
        assert runner.invoke(app, ["init"]).exit_code == 0
        Path("agentprof.yml").write_text(
            "pricing:\n"
            "  models:\n"
            "    - model: gpt-4o-mini\n"
            "      input_per_1m_usd: '9.99'\n"
            "      output_per_1m_usd: '8.88'\n",
            encoding="utf-8",
        )
        result = runner.invoke(app, ["pricing", "list"])

    assert result.exit_code == 0
    assert "9.99" in result.output
    # The shadowed bundled rate must not appear alongside the override.
    assert "0.15" not in result.output
