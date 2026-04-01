# Setup

## Prerequisites

- Python 3.12+ (`3.13` recommended)
- [Rust toolchain](https://rustup.rs/) `>= 1.93.1`
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

## Install

```bash
git clone https://github.com/evan-kolberg/prediction-market-backtesting.git
cd prediction-market-backtesting

# conda's linker flags conflict with the rust build
unset CONDA_PREFIX

uv venv --python 3.13
uv pip install -e nautilus_pm/ bokeh plotly numpy py-clob-client duckdb
```

You can also use:

```bash
make install
```

After setup, run commands with `uv run ...`. No manual
`source .venv/bin/activate` step is required.

## First Run

Interactive backtest menu:

```bash
make backtest
```

Direct entrypoint:

```bash
uv run python main.py
```

## PMXT Defaults

- PMXT fetch timing output is on by default in `make backtest` and
  `uv run python main.py`
- `BACKTEST_ENABLE_TIMING=0` is the explicit quiet opt-out
- local PMXT filtered cache is enabled by default at
  `~/.cache/nautilus_trader/pmxt`

## Updating The Vendored Subtree

```bash
make update
```

Unlike submodules, the subtree is copied into this repo. There is no live link
to upstream.
