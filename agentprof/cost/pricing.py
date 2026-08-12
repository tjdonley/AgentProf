from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal

from agentprof.config import PricingConfig
from agentprof.normalize.schema import NormalizedSpan


COST_QUANTUM = Decimal("0.000000001")
TOKENS_PER_PRICE_UNIT = Decimal("1000000")
CONFIG_PRICE_SOURCE = "config"
DEFAULT_PRICE_SOURCE = "default"
PRICE_SOURCE_RANK = {CONFIG_PRICE_SOURCE: 0, DEFAULT_PRICE_SOURCE: 1}
BOUNDARY_CHARS = frozenset("-_.:@/ ")

# Published list prices in USD per million tokens, recorded on this date. They
# drift: treat estimates built from this table as a starting point and override
# any model that matters with `pricing.models` in agentprof.yml.
DEFAULT_PRICE_TABLE_AS_OF = "2026-05-01"
DEFAULT_MODEL_PRICES: tuple[tuple[str, str, str], ...] = (
    # OpenAI
    ("gpt-4.1", "2.00", "8.00"),
    ("gpt-4.1-mini", "0.40", "1.60"),
    ("gpt-4.1-nano", "0.10", "0.40"),
    ("gpt-4o", "2.50", "10.00"),
    ("gpt-4o-mini", "0.15", "0.60"),
    ("gpt-3.5-turbo", "0.50", "1.50"),
    ("o3-mini", "1.10", "4.40"),
    # Anthropic
    ("claude-3-opus", "15.00", "75.00"),
    ("claude-3-5-sonnet", "3.00", "15.00"),
    ("claude-3-5-haiku", "0.80", "4.00"),
    ("claude-3-haiku", "0.25", "1.25"),
    ("claude-haiku-4-5", "1.00", "5.00"),
    ("claude-sonnet-4", "3.00", "15.00"),
    ("claude-sonnet-4-5", "3.00", "15.00"),
    ("claude-opus-4", "15.00", "75.00"),
    ("claude-opus-4-1", "15.00", "75.00"),
    # Google
    ("gemini-1.5-flash", "0.075", "0.30"),
    ("gemini-1.5-pro", "1.25", "5.00"),
    ("gemini-2.0-flash", "0.10", "0.40"),
)


@dataclass(frozen=True)
class ModelPrice:
    model: str
    input_per_1m_usd: Decimal
    output_per_1m_usd: Decimal
    source: str


@dataclass(frozen=True)
class SpanCostEstimate:
    cost_usd: Decimal
    price: ModelPrice


@dataclass(frozen=True)
class CostEstimationResult:
    """What estimation did to a batch of spans, for reporting back to the user."""

    estimated_spans: int = 0
    estimated_cost_usd: Decimal = Decimal("0")
    unpriced_models: tuple[str, ...] = ()
    priced_models: tuple[str, ...] = ()


@dataclass(frozen=True)
class PricingTable:
    """Model prices ordered so the most specific configured match wins."""

    prices: tuple[ModelPrice, ...] = ()
    _exact: dict[str, ModelPrice] = field(default_factory=dict, compare=False)

    def __bool__(self) -> bool:
        return bool(self.prices)

    def lookup(self, model_name: str | None) -> ModelPrice | None:
        if not model_name:
            return None

        keys = _candidate_keys(model_name)
        for key in keys:
            exact = self._exact.get(key)
            if exact is not None:
                return exact
        for key in keys:
            for price in self.prices:
                if _prefix_matches(key, price.model):
                    return price
        return None


def build_pricing_table(config: PricingConfig | None) -> PricingTable:
    """Merge configured prices over the bundled defaults."""

    if config is None or not config.enabled:
        return PricingTable()

    prices: list[ModelPrice] = [
        ModelPrice(
            model=_normalize_model_name(entry.model),
            input_per_1m_usd=entry.input_per_1m_usd,
            output_per_1m_usd=entry.output_per_1m_usd,
            source=CONFIG_PRICE_SOURCE,
        )
        for entry in config.models
    ]
    if config.use_default_table:
        prices.extend(
            ModelPrice(
                model=_normalize_model_name(model),
                input_per_1m_usd=Decimal(input_rate),
                output_per_1m_usd=Decimal(output_rate),
                source=DEFAULT_PRICE_SOURCE,
            )
            for model, input_rate, output_rate in DEFAULT_MODEL_PRICES
        )

    exact: dict[str, ModelPrice] = {}
    for price in prices:
        exact.setdefault(price.model, price)

    ordered = tuple(
        sorted(
            prices,
            key=lambda price: (
                -len(price.model),
                PRICE_SOURCE_RANK.get(price.source, len(PRICE_SOURCE_RANK)),
                price.model,
            ),
        )
    )
    return PricingTable(prices=ordered, _exact=exact)


def estimate_span_cost(
    span: NormalizedSpan, table: PricingTable
) -> SpanCostEstimate | None:
    """Price one span from its token counts, or return None if it can't be priced."""

    if span.cost_usd is not None:
        return None

    input_tokens = span.input_tokens or 0
    output_tokens = span.output_tokens or 0
    if input_tokens <= 0 and output_tokens <= 0:
        return None

    price = table.lookup(span.model_name)
    if price is None:
        return None

    cost = (
        Decimal(input_tokens) * price.input_per_1m_usd
        + Decimal(output_tokens) * price.output_per_1m_usd
    ) / TOKENS_PER_PRICE_UNIT
    return SpanCostEstimate(
        cost_usd=cost.quantize(COST_QUANTUM, rounding=ROUND_HALF_UP),
        price=price,
    )


def apply_estimated_costs(
    spans: Sequence[NormalizedSpan], table: PricingTable
) -> tuple[list[NormalizedSpan], CostEstimationResult]:
    """Fill in missing span costs from token counts.

    Spans that already carry a provider cost are never touched, so estimation
    can only add attribution where there was none.
    """

    if not table:
        return list(spans), CostEstimationResult(
            unpriced_models=_unpriced_model_names(spans, table)
        )

    updated: list[NormalizedSpan] = []
    estimated_spans = 0
    estimated_cost = Decimal("0")
    priced_models: set[str] = set()

    for span in spans:
        estimate = estimate_span_cost(span, table)
        if estimate is None:
            updated.append(span)
            continue

        updated.append(
            span.model_copy(
                update={
                    "cost_usd": estimate.cost_usd,
                    "cost_confidence": "estimated",
                }
            )
        )
        estimated_spans += 1
        estimated_cost += estimate.cost_usd
        if span.model_name:
            priced_models.add(_normalize_model_name(span.model_name))

    return updated, CostEstimationResult(
        estimated_spans=estimated_spans,
        estimated_cost_usd=estimated_cost,
        unpriced_models=_unpriced_model_names(spans, table),
        priced_models=tuple(sorted(priced_models)),
    )


def _unpriced_model_names(
    spans: Sequence[NormalizedSpan], table: PricingTable
) -> tuple[str, ...]:
    """Models that had usable token counts but no matching price."""

    names: set[str] = set()
    for span in spans:
        if span.cost_usd is not None or not span.model_name:
            continue
        if (span.input_tokens or 0) <= 0 and (span.output_tokens or 0) <= 0:
            continue
        if table.lookup(span.model_name) is None:
            names.add(_normalize_model_name(span.model_name))
    return tuple(sorted(names))


def _candidate_keys(model_name: str) -> tuple[str, ...]:
    key = _normalize_model_name(model_name)
    if not key:
        return ()
    if "/" in key:
        return (key, key.rsplit("/", 1)[-1])
    return (key,)


def _prefix_matches(key: str, model: str) -> bool:
    """Match on a version-boundary so `gpt-4` never prices a `gpt-4o` span."""

    if not key.startswith(model):
        return False
    remainder = key[len(model) :]
    return not remainder or remainder[0] in BOUNDARY_CHARS


def _normalize_model_name(model_name: str) -> str:
    return " ".join(model_name.split()).lower()
