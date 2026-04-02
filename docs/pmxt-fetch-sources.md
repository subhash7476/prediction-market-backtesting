# PMXT Data Fetch Sources and Timing

When running a backtest, the PMXT loader fetches historical L2 order book data one hour at a time. Each hour can come from one of six sources, tried in order:

1. **Local cache** (`~/.cache/nautilus_trader/pmxt/...`) — cached from a previous run. Sub-millisecond reads. The source column shows the full local file path.
2. **Local raw PMXT archive** (`PMXT_LOCAL_ARCHIVE_DIR`) — a locally mirrored raw PMXT hour file. The loader filters it client-side without touching the network.
3. **Relay prebuilt** (`https://209-209-10-83.sslip.io`) — pre-partitioned per market/token file served directly from the relay. Under 2 seconds typically.
4. **Raw PMXT archive** (`https://r2.pmxt.dev`) — the hour isn't available from cache, local raws, or relay prebuilt files, so the loader falls back to the upstream PMXT archive. The client streams the raw parquet into a temporary local file, filters it locally, then deletes the temp file.
5. **Relay raw passthrough** (`https://209-209-10-83.sslip.io/v1/raw/...`) — the hour is mirrored on the relay but not yet prebuilt for your market/token, and upstream PMXT wasn't the winning source. The client downloads the raw relay parquet into a temporary local file, filters it locally, then deletes the temp file.
6. **None** — the hour doesn't exist on any source (future hour, no data yet). Returns 0 rows.

If a source fails or returns nothing, the loader falls through to the next one. After a successful fetch from sources 2-5, the result is written to the local cache so subsequent runs are instant.

The two remote raw tiers do not accumulate local junk. They use a scratch
directory under your system temp folder, scan the downloaded parquet locally,
then clean it up immediately after the hour finishes.

Caching is enabled by default at `~/.cache/nautilus_trader/pmxt/`. To disable it, set `PMXT_DISABLE_CACHE=true`. If you already have a local raw mirror, set `PMXT_LOCAL_ARCHIVE_DIR=/path/to/raw-hours` to keep that disk-local tier ahead of every remote source.

Hours print in completion order (not chronological) because they're fetched concurrently with 16 workers.

## Example output

This is representative output from a fixed-window 120-hour deep value backtest on
the `will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026`
market, run on 2026-03-24. The full real run was much longer; this trimmed
snippet shows the mix of cache, relay prebuilt, `r2.pmxt.dev`, and the summary
without dumping an entire terminal screen into the page.

```text
Loading PMXT Polymarket market will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026 (token_index=0, window_start=2026-03-19T07:35:57.277659+00:00, window_end=2026-03-24T07:35:57.277659+00:00, window_hours=120.0)...
  2026-03-19T11:00:00+00:00   0.001s     214 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/.../polymarket_orderbook_2026-03-19T11.parquet
  2026-03-20T18:00:00+00:00   0.532s      25 rows  https://209-209-10-83.sslip.io
  2026-03-20T03:00:00+00:00  41.513s    2443 rows  https://r2.pmxt.dev
  2026-03-20T05:00:00+00:00  45.111s      91 rows  https://r2.pmxt.dev
  2026-03-20T04:00:00+00:00  47.286s     148 rows  https://r2.pmxt.dev
  ...
  2026-03-24T07:00:00+00:00   0.404s       0 rows  none
Fetching hours: 100%|██████████████████████████████████████| 122/122 [00:50<00:00]

Market                                                                  Quotes  Fills   PnL (USDC)
will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026      32281      2      -1.4700

Total wall time: 54.36s
```

122 hours fetched in ~54 seconds. The three hours that hit `r2.pmxt.dev` (41-47s each) dominated the total time. Everything else was either cached locally (<0.05s) or served from the relay's prebuilt files (<1.6s). On a second run, every hour comes from cache and the same backtest completes in under a second.

## Timing expectations by source

| Source | Typical time | When it happens |
|---|---|---|
| Local cache | <0.05s | Second run onward (same market/token/hour) |
| Local raw PMXT archive | local disk bound | You mirrored raw PMXT hours locally and set `PMXT_LOCAL_ARCHIVE_DIR` |
| Relay prebuilt | 0.4-1.6s | Hour has been fully prebuilt on the relay |
| Raw PMXT archive (r2.pmxt.dev) | network and file-size bound | Hour missing from relay sources, so the client downloads the upstream raw parquet to a temp file and filters it locally |
| Relay raw passthrough | network and file-size bound | Hour is mirrored on the relay but not yet prebuilt for your market/token, so the client downloads the raw relay parquet to a temp file and filters it locally |
| None | <1s | Hour doesn't exist yet |

## How to see this output

The timing instrumentation is enabled by default in `make backtest`.
Turn it off explicitly with `BACKTEST_ENABLE_TIMING=0`, or run the timing
harness standalone against any backtest file:

```bash
BACKTEST_ENABLE_TIMING=0 make backtest
```

Or:

```bash
uv run python backtests/_shared/_timing_test.py backtests/polymarket_quote_tick/polymarket_pmxt_relay_ema_crossover.py
```

The PMXT single-market relay runners are pinned in code to one known-good
historical slice:

```bash
uv run python backtests/_shared/_timing_test.py backtests/polymarket_quote_tick/polymarket_pmxt_relay_breakout.py
```

Today that slice is `will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026`
from `2026-03-19T07:35:57.277659Z` through `2026-03-24T07:35:57.277659Z`
(120 hours).
