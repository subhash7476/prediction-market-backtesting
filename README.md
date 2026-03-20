# prediction-market-backtesting

![GitHub stars](https://img.shields.io/github/stars/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub forks](https://img.shields.io/github/forks/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub watchers](https://img.shields.io/github/watchers/evan-kolberg/prediction-market-backtesting?style=social)

[![Licensing: Mixed](https://img.shields.io/badge/licensing-MIT%20%2B%20LGPL--3.0--or--later-blue.svg)](NOTICE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
![Python](https://img.shields.io/badge/python-3.12%2B-3776AB?logo=python&logoColor=white)
![Rust](https://img.shields.io/badge/rust-1.93.1-CE422B?logo=rust&logoColor=white)
![Rust Edition](https://img.shields.io/badge/edition-2024-CE422B?logo=rust&logoColor=white)
![NautilusTrader](https://img.shields.io/badge/NautilusTrader-1.224.0-1E3A5F)
![GitHub last commit](https://img.shields.io/github/last-commit/evan-kolberg/prediction-market-backtesting)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/evan-kolberg/prediction-market-backtesting)
![GitHub code size](https://img.shields.io/github/languages/code-size/evan-kolberg/prediction-market-backtesting)
![GitHub top language](https://img.shields.io/github/languages/top/evan-kolberg/prediction-market-backtesting)
![GitHub open issues](https://img.shields.io/github/issues/evan-kolberg/prediction-market-backtesting)

Backtesting framework for prediction market trading strategies on [Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com), built off of [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) with custom exchange adapters.

> Miss the old engine? See the [`legacy`](https://github.com/evan-kolberg/prediction-market-backtesting/tree/legacy) branch. Though, I don't recommend you continue using that one. 

Fantastic single & multi-market charting. Featuring: equity (total & individual markets), profit / loss ticks, P&L periodic bars, market allocation, YES price (with green buy and red sell fills), drawdown, sharpe (with above/below shading), cash / equity, monthly returns, and cumulative brier advantage.
![Image](https://github.com/user-attachments/assets/e9b00915-9413-42d8-aeff-c2bde627c3d8)

> IMPORTANT: Kalshi public backtests here are still bar/trade replay only. Polymarket now also has a PMXT-backed historical L2 path which replays hourly PMXT order-book events through NautilusTrader's `L2_MBP` matching engine. That is much better than the old one-tick slippage proxy for taker-style execution, but it still does **not** fully solve passive-order realism because public L2 MBP data does not reveal true queue position. Also, pulling those historical order book archives will cause the backtests to take a long time, especially for multiple markets. 

## Table of Contents

- [Setup](#setup)
- [Writing Strategies and Backtests](#writing-strategies-and-backtests)
- [Running Backtests](#running-backtests)
- [Execution Modeling](#execution-modeling)
- [Plotting](#plotting)
- [Testing](#testing)
- [Updating the Subtree](#updating-the-subtree)
- [Roadmap](#roadmap)
- [Known Issues](#known-issues)
- [License](#license)
  - [What is LGPL-3.0-or-later](#what-is-lgpl-30-or-later)
  - [NautilusTrader attribution](#nautilustrader-attribution)
  - [What this means for you](#what-this-means-for-you)


## Setup

### Prerequisites

- Python 3.12+ (3.13 recommended)
- [Rust toolchain](https://rustup.rs/) ≥ 1.93.1 — required to build NautilusTrader's Cython/Rust extensions
- [uv](https://docs.astral.sh/uv/getting-started/installation/) — for virtual environment and package management

### Install

```bash
git clone https://github.com/evan-kolberg/prediction-market-backtesting.git
cd prediction-market-backtesting

# conda's linker flags conflict with the rust build
unset CONDA_PREFIX

# create a venv and install everything
# compiling the rust & cython extensions will take a hot minute
uv venv --python 3.13
uv pip install -e nautilus_pm/ bokeh plotly numpy py-clob-client
```

You can also use:

```bash
make install
```

After setup, use `uv run python <script>` to run anything. No manual `source .venv/bin/activate` is needed.

## Writing Strategies and Backtests

This repo now has a hard split:

- `strategies/` contains reusable strategy classes and configs.
- `strategies/private/` is for git-ignored local strategy modules.
- `backtests/` contains runnable backtest entrypoints and orchestration helpers.
- `backtests/private/` is for git-ignored local backtest runners.

Good public examples:

- Reusable EMA strategy logic: [`strategies/ema_crossover.py`](strategies/ema_crossover.py)
- Reusable final-period momentum logic: [`strategies/final_period_momentum.py`](strategies/final_period_momentum.py)
- Reusable late-favorite limit-hold logic: [`strategies/late_favorite_limit_hold.py`](strategies/late_favorite_limit_hold.py)
- Kalshi runner using a root strategy module: [`backtests/kalshi_breakout.py`](backtests/kalshi_breakout.py)
- Polymarket runner using a root strategy module: [`backtests/polymarket_vwap_reversion.py`](backtests/polymarket_vwap_reversion.py)
- PMXT Polymarket quote-tick runners live under `backtests/polymarket_pmxt_*.py`.
  Current public runners cover EMA crossover, breakout, RSI reversion, spread
  capture, panic fade, VWAP reversion, threshold momentum, final-period
  momentum, deep value, and late-favorite limit hold.
- Public multi-market runner: [`backtests/polymarket_sports_final_period_momentum.py`](backtests/polymarket_sports_final_period_momentum.py)
- Public resolved multi-market runner with settlement-adjusted PnL: [`backtests/polymarket_sports_late_favorite_limit_hold.py`](backtests/polymarket_sports_late_favorite_limit_hold.py), [`backtests/polymarket_sports_vwap_reversion.py`](backtests/polymarket_sports_vwap_reversion.py)

Backtest entrypoints should expose three things at module level:

```python
NAME = "my_strategy"           # shown in the menu
DESCRIPTION = "one-liner"      # shown in the menu

async def run() -> None:       # called when selected
    ...
```

Use the root `strategies` package for signal logic, then import that logic into a thin backtest runner. Export new reusable configs and classes from [`strategies/__init__.py`](strategies/__init__.py) so runners can import them cleanly.

Three common runner patterns already exist:

- Kalshi bar backtests via [`backtests/_kalshi_single_market_runner.py`](backtests/_kalshi_single_market_runner.py)
- Polymarket trade-tick backtests via [`backtests/_polymarket_single_market_runner.py`](backtests/_polymarket_single_market_runner.py)
- Polymarket PMXT L2 backtests via [`backtests/_polymarket_single_market_pmxt_runner.py`](backtests/_polymarket_single_market_pmxt_runner.py)

## Running Backtests

Interactive menu:

```bash
make backtest
```

Any module in `backtests/` or `backtests/private/` with `NAME`, `DESCRIPTION`, and `async def run()` shows up here.

Equivalent direct command:

```bash
uv run python main.py
```

Direct script execution is usually better once you know which runner you want:

```bash
MARKET_TICKER=KXNEXTIRANLEADER-45JAN01-MKHA uv run python backtests/kalshi_breakout.py
MARKET_SLUG=will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026 uv run python backtests/polymarket_vwap_reversion.py
MARKET_SLUG=will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026 END_TIME=2026-03-15T18:00:00Z LOOKBACK_HOURS=4 uv run python -m backtests.polymarket_pmxt_ema_crossover
MARKET_SLUGS=nfl-was-gb-2025-09-11,nfl-nyj-cin-2025-10-26 TARGET_RESULTS=2 uv run python backtests/polymarket_sports_final_period_momentum.py
TARGET_RESULTS=50 uv run python -m backtests.polymarket_sports_late_favorite_limit_hold
```

These hit live APIs. Expect latency and rate limits.

Most runners are configured through environment variables. Common ones:

- `MARKET_TICKER` for Kalshi single-market runners
- `MARKET_SLUG` for Polymarket single-market runners
- `LOOKBACK_DAYS` for data window size
- `LOOKBACK_HOURS` for PMXT L2 runners
- `TRADE_SIZE` and `INITIAL_CASH` for sizing
- `TARGET_RESULTS` for multi-market runners

## Execution Modeling

Backtests here replay venue data from Kalshi and Polymarket into NautilusTrader.
The main things which affect realized backtest performance beyond the raw API
data are:

- exchange fee models
- slippage for taker-style orders
- existing engine behavior such as IOC handling, price rounding, cash-account limits, and `AccountBalanceNegative` stops

### Fees

- Kalshi uses a nonlinear expected-earnings fee model.
- Polymarket uses the venue fee model plus CLOB `fee-rate` enrichment when the
  market payload itself reports zero fees.
- If a venue reports zero fees for a market, the backtest also applies zero fees.

### Slippage

- Shared prediction-market backtests default to a custom taker fill model.
- Non-limit orders get a deterministic one-tick adverse fill.
- Polymarket uses the market's own tick size.
- Kalshi uses one cent as the effective order tick for taker slippage.
- Limit orders keep the default Nautilus matching behavior and do not get the
  forced one-tick adverse move.
- PMXT-backed Polymarket L2 backtests do **not** use the synthetic one-tick
  taker fill model. They replay historical `OrderBookDeltas` with
  `book_type=L2_MBP` and `liquidity_consumption=True`.

### Limits

- This is a conservative taker-execution proxy, not full order-book replay.
- Historical backtests here do not model queue position, full L2 depth, or
  exact partial-sweep behavior.
- Taker-heavy strategies that try to harvest very small price changes can look
  much worse once fees and one-tick slippage are turned on.
- PMXT improves Polymarket taker fills materially, but passive-order fills are
  still approximate because public L2 MBP data cannot tell you your exact place
  in queue.

### PMXT Polymarket L2

- PMXT data comes from hourly parquet files on `r2.pmxt.dev`.
- The loader scans one extra hour before the requested start so it can find a
  valid snapshot and rebuild the book. File count is therefore roughly
  `LOOKBACK_HOURS + 2`.
- Example file counts:
  - `LOOKBACK_HOURS=2` scans 4 hourly files
  - `LOOKBACK_HOURS=4` scans 6 hourly files
  - `LOOKBACK_HOURS=24` scans 26 hourly files
  - `LOOKBACK_HOURS=48` scans 50 hourly files
- For each hour, the loader opens the remote parquet file over HTTPS and pushes
  down filters for:
  - `market_id == <condition_id>`
  - `update_type in {"book_snapshot", "price_change"}`
- Remote HTTPS reads use in-memory readahead by default with `32 MiB` blocks to
  reduce small-range-request overhead without writing raw PMXT files to disk.
- The PMXT parquet schema does not expose `token_id` as its own column. The
  token lives inside the JSON payload in the `data` column, so token filtering
  happens after the market-level parquet scan.
- The loader now processes filtered Arrow record batches incrementally instead
  of materializing full hourly tables before decode, which reduces cold-load
  Python overhead and peak memory during remote scans.
- Each surviving JSON payload is decoded into Nautilus
  `OrderBookDeltas`/`QuoteTick` records, and the backtest waits for that full
  ingest to finish before strategy execution starts.
- Local PMXT disk cache is optional. By default the loader remote-scans the
  hourly parquet files and does not persist PMXT data to disk. If
  `PMXT_CACHE_DIR` is set, the loader writes the filtered hourly parquet table
  for one market/token/hour to:

```text
~/.cache/nautilus_trader/pmxt/<condition_id>/<token_id>/polymarket_orderbook_YYYY-MM-DDTHH.parquet
```

- Reuse rules:
  - same market, same token, same hour: cache hit
  - same market, same token, overlapping window: overlapping cached hours are reused
  - same hour, different market: no reuse, because the cache key includes `condition_id`
  - same market, different token/outcome: no reuse, because the cache key includes `token_id`
- This means repeated runs of the same PMXT market can get much faster after
  the first pass if disk cache is enabled, but multi-market runs still pay
  separate remote reads for the same UTC hour because each market builds its
  own filtered hourly cache.
- The loader also prefetches multiple hours in parallel while still yielding
  them back in chronological order. Configure that with
  `PMXT_PREFETCH_WORKERS` (default `4`).
- Cache controls:
  - `PMXT_CACHE_DIR=1` enables disk cache at `~/.cache/nautilus_trader/pmxt`
  - `PMXT_CACHE_DIR=/custom/path` enables disk cache at a custom root
  - `PMXT_DISABLE_CACHE=1` disables local PMXT disk cache entirely
  - `PMXT_PREFETCH_WORKERS=8` increases hourly prefetch parallelism
  - `PMXT_HTTP_BLOCK_SIZE_MB=64` increases the in-memory HTTP readahead block size
  - `PMXT_HTTP_CACHE_TYPE=bytes` switches the HTTP file cache strategy
- Cache size is currently unbounded. There is no eviction policy or size cap.
  If disk cache is enabled, it grows with the number of unique
  `(condition_id, token_id, hour)` tuples you backtest. Very active markets and
  long lookbacks will produce larger cached parquet files than quiet markets
  and short windows. Check current size with:

```bash
du -sh ~/.cache/nautilus_trader/pmxt
```

- Example: turn on disk cache for a PMXT backtest and use the default cache
  location:

```bash
PMXT_CACHE_DIR=1 END_TIME=2026-03-16T13:00:00Z LOOKBACK_HOURS=2 MIN_PRICE_RANGE=0 TRADE_SIZE=10 \
  uv run python backtests/polymarket_pmxt_ema_crossover.py
```

- Validation on 2026-03-19 using the OpenAI hardware market
  `will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026`
  with `END_TIME=2026-03-16T13:00:00Z`, `LOOKBACK_HOURS=2`,
  `MIN_PRICE_RANGE=0`, `TRADE_SIZE=10`, and `PMXT_CACHE_DIR=1`:
  - the first `polymarket_pmxt_ema_crossover.py` run populated `4` cached
    hourly parquet files in about `17.0s`
  - the warmed cache footprint for that slice was about `116 KB`
  - all `backtests/polymarket_pmxt_*.py` entrypoints then exited `0` on the
    same cached slice
  - warm cached end-to-end runtimes ranged from about `2.0s` to `3.1s`
  - each validated PMXT backtest loaded `1286` quotes on that slice
- Validation on 2026-03-19 using the same market over 48 hours
  (`END_TIME=2026-03-16T13:00:00Z`, `LOOKBACK_HOURS=48`,
  `MIN_PRICE_RANGE=0`, `TRADE_SIZE=1`, `PMXT_CACHE_DIR=1`):
  - the first `polymarket_pmxt_ema_crossover.py` run populated `50` cached
    hourly parquet files in about `12m 50s` end to end
  - the warmed cache footprint for that full 48-hour slice was about `1.6 MB`
  - fresh warm cached end-to-end reruns on the same slice:
    `ema_crossover` about `1.1s` with `32819` quotes, `26` fills, `PnL -0.0880`
    `rsi_reversion` about `2.1s` with `32819` quotes, `198` fills, `PnL -0.3610`
    `spread_capture` about `1.0s` with `32819` quotes, `50` fills, `PnL -0.1808`
  - repeated runs of that same market/token/window can now reuse overlapping
    cached hours from local disk instead of re-downloading them
  - different markets covering the same UTC hours still do not share cache
    entries, because the cache is market/token scoped
- Cold uncached PMXT runs still have to remote-scan every unique hour in the
  requested window, so first-run load time remains dominated by PMXT archive
  access even after the batch-streaming and HTTP readahead improvements.
- Short windows can still fail if the selected range never includes usable L2
  book state for that instrument.

  <img width="751" height="434" alt="Image" src="https://github.com/user-attachments/assets/4dcfa115-4785-4a7e-ac10-41a4bfb3f8eb" />

## Plotting

Single-market plotting is built into the shared runner flow used by the public prediction-market backtests. Good examples:

- [`backtests/kalshi_breakout.py`](backtests/kalshi_breakout.py)
- [`backtests/kalshi_panic_fade.py`](backtests/kalshi_panic_fade.py)
- [`backtests/polymarket_panic_fade.py`](backtests/polymarket_panic_fade.py)
- [`backtests/polymarket_vwap_reversion.py`](backtests/polymarket_vwap_reversion.py)

These write HTML charts to `output/`, typically with names like `output/<backtest>_<market>_legacy.html`.

PMXT single-market runners do the same. Example outputs:

- `output/polymarket_pmxt_ema_crossover_<market>_legacy.html`
- `output/polymarket_pmxt_breakout_<market>_legacy.html`
- `output/polymarket_pmxt_rsi_reversion_<market>_legacy.html`
- `output/polymarket_pmxt_spread_capture_<market>_legacy.html`

Multi-market plotting example:

- [`backtests/polymarket_sports_final_period_momentum.py`](backtests/polymarket_sports_final_period_momentum.py)
- [`backtests/polymarket_sports_vwap_reversion.py`](backtests/polymarket_sports_vwap_reversion.py)

By default that script:

- runs repeated single-market backtests,
- writes per-market legacy charts to `output/`, and
- writes an aggregate multi-market chart to `output/polymarket_sports_final_period_momentum_multi_market.html`.

Optional combined-report output is available with:

```bash
COMBINED_REPORT=true uv run python backtests/polymarket_sports_final_period_momentum.py
```

That writes `output/polymarket_sports_final_period_momentum_combined_legacy.html`.

## Testing

```bash
make test
```

Runs the end-to-end test suite against the live APIs. Each test redirects generated legacy-chart output to an isolated pytest temp directory so nothing in the working tree is mutated.

## Updating the Subtree

```bash
make update
```

Unlike git submodules, subtrees copy upstream code directly into this repo — there's no live link. `make update` currently pulls the upstream `nautilus_pm` `charting` branch.

## Roadmap

- [ ] Live paper trading mode
- [x] multi-market support within strategies
- [x] better position sizing capabilities
- [x] fee modeling [PR#4](https://github.com/ben-gramling/nautilus_pm/pull/4)
- [ ] total slippage modeling *** [PR#6](https://github.com/ben-gramling/nautilus_pm/pull/6), [PR#9](https://github.com/evan-kolberg/prediction-market-backtesting/pull/9)
- [x] polymarket L2 order book backtests [PR#10](https://github.com/evan-kolberg/prediction-market-backtesting/pull/10)
- [ ] kalshi L2 order book backtests
- [x] much better & informative charting [PR#5](https://github.com/ben-gramling/nautilus_pm/pull/5)


## Known Issues

- [ ] APIs rate-limit a lot. Kalshi seems worse. (for trade tick config)
- [ ] Cold PMXT L2 loads still take a long time; multi-market runs do not yet share a raw per-hour cache, and optional filtered-disk-cache growth is currently unbounded

## License

This repository uses **mixed licensing** because it vendors and extends
[NautilusTrader](https://github.com/nautechsystems/nautilus_trader), which is
licensed under the
[GNU Lesser General Public License v3.0 or later (LGPL-3.0-or-later)](https://www.gnu.org/licenses/lgpl-3.0.en.html).

### What is LGPL-3.0-or-later

| Scope | License | File |
|---|---|---|
| `nautilus_pm/` (vendored NautilusTrader subtree) | LGPL-3.0-or-later | [`nautilus_pm/LICENSE`](nautilus_pm/LICENSE) |
| Root files with a "Derived from NautilusTrader" or "Modified by Evan Kolberg" notice (most of `backtests/`, `strategies/`, and select `tests/` files) | LGPL-3.0-or-later | [`COPYING.LESSER`](COPYING.LESSER), [`COPYING`](COPYING) |
| Everything else (`main.py`, `Makefile`, docs, repo metadata, etc.) | MIT | [`LICENSE-MIT`](LICENSE-MIT) |

The full LGPL and GPL license texts are in [`COPYING.LESSER`](COPYING.LESSER) and
[`COPYING`](COPYING). The [`NOTICE`](NOTICE) file lists every LGPL-covered file
outside the subtree, along with modification dates and upstream lineage.

### NautilusTrader attribution

This project includes a vendored copy of
[NautilusTrader](https://github.com/nautechsystems/nautilus_trader)
(Copyright 2015-2026 Nautech Systems Pty Ltd) under `nautilus_pm/`. The upstream
LGPL-3.0-or-later license and copyright notices are preserved in
[`nautilus_pm/LICENSE`](nautilus_pm/LICENSE). All files modified or added within
the vendored subtree carry dated file-level notices identifying the changes. The
most recent subtree sync references upstream split commit `f51c805c9f`.

### What this means for you

- **Using this repo as-is**: no action needed. The LGPL obligations are satisfied
  by the notices already present.
- **Forking or redistributing**: you must keep the LGPL license files, the
  [`NOTICE`](NOTICE), and the per-file modification headers intact. Any further
  modifications to LGPL-covered files must also be noted.
- **Linking as a library**: if you import LGPL-covered modules into your own
  proprietary project, the LGPL requires that users can re-link against modified
  versions of the LGPL code. See the
  [LGPL FAQ](https://www.gnu.org/licenses/gpl-faq.html#LGPLStaticVsDynamic) for
  details.

See [`LICENSE`](LICENSE) for the top-level guide, or [`NOTICE`](NOTICE) for the
full file-by-file breakdown.

---

## Star History

<a href="https://www.star-history.com/#evan-kolberg/prediction-market-backtesting&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=evan-kolberg/prediction-market-backtesting&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=evan-kolberg/prediction-market-backtesting&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=evan-kolberg/prediction-market-backtesting&type=date&legend=top-left" />
 </picture>
</a>
