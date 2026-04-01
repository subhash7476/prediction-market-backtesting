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

Relay VPS statistics:

[![PMXT relay](https://209-209-10-83.sslip.io/v1/badge/status.svg)](https://209-209-10-83.sslip.io/v1/stats)
[![Relay CPU](https://209-209-10-83.sslip.io/v1/badge/cpu.svg)](https://209-209-10-83.sslip.io/v1/system)
[![Relay mem](https://209-209-10-83.sslip.io/v1/badge/mem.svg)](https://209-209-10-83.sslip.io/v1/system)
[![Relay disk](https://209-209-10-83.sslip.io/v1/badge/disk.svg)](https://209-209-10-83.sslip.io/v1/system)

[![PMXT mirrored](https://209-209-10-83.sslip.io/v1/badge/mirrored.svg)](https://209-209-10-83.sslip.io/v1/stats)
[![PMXT processed](https://209-209-10-83.sslip.io/v1/badge/processed.svg)](https://209-209-10-83.sslip.io/v1/stats)
[![PMXT latest](https://209-209-10-83.sslip.io/v1/badge/latest.svg?v=3)](https://209-209-10-83.sslip.io/v1/queue)
[![PMXT lag](https://209-209-10-83.sslip.io/v1/badge/lag.svg?v=3)](https://209-209-10-83.sslip.io/v1/queue)
[![PMXT rate](https://209-209-10-83.sslip.io/v1/badge/rate.svg?v=1)](https://209-209-10-83.sslip.io/v1/stats)

[![PMXT file](https://209-209-10-83.sslip.io/v1/badge/prebuild-file.svg?v=1)](https://209-209-10-83.sslip.io/v1/events?limit=50)
[![PMXT rows](https://209-209-10-83.sslip.io/v1/badge/prebuild-progress.svg?v=1)](https://209-209-10-83.sslip.io/v1/events?limit=50)

Backtesting framework for prediction market strategies on
[Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com), built on
top of [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) with
custom exchange adapters. Plotting inspired by [minitrade](https://github.com/dodid/minitrade). This repo is still in active development, and a full release should happen within the next one to two months.

Fantastic single & multi-market charting. Featuring: equity (total & individual markets), profit / loss ticks, P&L periodic bars, market allocation, YES price (with green buy and red sell fills), drawdown, sharpe (with above/below shading), cash / equity, monthly returns, and cumulative brier advantage.
![Charting preview](https://raw.githubusercontent.com/evan-kolberg/prediction-market-backtesting/main/docs/assets/charting-preview.jpeg)
[```/nautilus_pm/nautilus_trader/analysis/legacy_backtesting/plotting.py```](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/nautilus_pm/nautilus_trader/analysis/legacy_backtesting/plotting.py)


Detailed guides have been filed away in the [docs index](docs/index.md) for better organization and long-term sustainability.

## Table of Contents

- [Docs Index](docs/index.md)
- [Setup](docs/setup.md)
  - [Prerequisites](docs/setup.md#prerequisites)
  - [Install](docs/setup.md#install)
  - [First Run](docs/setup.md#first-run)
  - [PMXT Defaults](docs/setup.md#pmxt-defaults)
  - [Updating The Vendored Subtree](docs/setup.md#updating-the-vendored-subtree)
- [Backtests And Runners](docs/backtests.md)
  - [Repo Layout](docs/backtests.md#repo-layout)
  - [Runner Contract](docs/backtests.md#runner-contract)
  - [Running Backtests](docs/backtests.md#running-backtests)
  - [Common Environment Variables](docs/backtests.md#common-environment-variables)
  - [PMXT Notes](docs/backtests.md#pmxt-notes)
- [Execution Modeling](docs/execution-modeling.md)
  - [Fees](docs/execution-modeling.md#fees)
  - [Slippage](docs/execution-modeling.md#slippage)
  - [Limits](docs/execution-modeling.md#limits)
  - [PMXT L2 Behavior](docs/execution-modeling.md#pmxt-l2-behavior)
- [PMXT BYOD And Local Data](docs/pmxt-byod.md)
  - [What Works Today](docs/pmxt-byod.md#what-works-today)
  - [Supported Local File Layout](docs/pmxt-byod.md#supported-local-file-layout)
  - [Required Parquet Columns](docs/pmxt-byod.md#required-parquet-columns)
  - [Required JSON Payload Shape](docs/pmxt-byod.md#required-json-payload-shape)
  - [Relay Mode](docs/pmxt-byod.md#relay-mode)
  - [What Is Not Plug-And-Play Yet](docs/pmxt-byod.md#what-is-not-plug-and-play-yet)
- [PMXT Fetch Sources And Timing](docs/pmxt-fetch-sources.md)
  - [Example Output](docs/pmxt-fetch-sources.md#example-output)
  - [Timing Expectations By Source](docs/pmxt-fetch-sources.md#timing-expectations-by-source)
  - [How To See This Output](docs/pmxt-fetch-sources.md#how-to-see-this-output)
- [PMXT Relay](docs/pmxt-relay.md)
- [Plotting](docs/plotting.md)
  - [Output Paths](docs/plotting.md#output-paths)
  - [Multi-Market References](docs/plotting.md#multi-market-references)
- [Testing](docs/testing.md)
  - [Standard Repo Gate](docs/testing.md#standard-repo-gate)
  - [Useful Smoke Checks](docs/testing.md#useful-smoke-checks)
- [Project Status](docs/project-status.md)
  - [Roadmap](docs/project-status.md#roadmap)
  - [Known Issues](docs/project-status.md#known-issues)
  - [Recently Fixed](docs/project-status.md#recently-fixed)
- [License Notes](docs/license.md)
  - [Scope](docs/license.md#scope)
  - [NautilusTrader Attribution](docs/license.md#nautilustrader-attribution)
  - [Practical Meaning](docs/license.md#practical-meaning)


## Star History

<a href="https://www.star-history.com/?repos=evan-kolberg%2Fprediction-market-backtesting&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/image?repos=evan-kolberg/prediction-market-backtesting&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/image?repos=evan-kolberg/prediction-market-backtesting&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/image?repos=evan-kolberg/prediction-market-backtesting&type=date&legend=top-left" />
 </picture>
</a>
