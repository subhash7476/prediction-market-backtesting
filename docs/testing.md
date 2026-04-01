# Testing

## Standard Repo Gate

Run these before opening or merging a PR:

```bash
uv run ruff check --exclude nautilus_pm .
uv run ruff format --check --exclude nautilus_pm .
uv run pytest tests/ -q
```

You can also use:

```bash
make test
```

## Useful Smoke Checks

```bash
uv run python backtests/kalshi_trade_tick/kalshi_breakout.py
uv run python backtests/polymarket_trade_tick/polymarket_vwap_reversion.py
uv run python backtests/polymarket_quote_tick/polymarket_pmxt_relay_ema_crossover.py
```

Coverage is mixed by design:

- fast unit tests for strategy, loader, cache, and relay logic
- relay processor and API integration tests against temp dirs
- smoke tests that exercise real backtest flows
- generated chart output redirected to temp dirs during tests so the working
  tree stays clean
