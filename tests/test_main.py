from __future__ import annotations

import re
import sys
from types import SimpleNamespace

import main as main_module


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def test_main_installs_timing_patch_by_default(monkeypatch):
    calls = {"timing": 0, "run": 0}

    async def _run() -> None:
        calls["run"] += 1

    monkeypatch.setattr(
        main_module,
        "discover",
        lambda: [{"name": "demo", "description": "", "run": _run}],
    )
    monkeypatch.setattr(main_module, "show_menu", lambda _backtests: 0)
    monkeypatch.delenv(main_module.ENABLE_TIMING_ENV, raising=False)
    monkeypatch.setitem(
        sys.modules,
        "backtests._shared._timing_test",
        SimpleNamespace(
            install_timing=lambda: calls.__setitem__("timing", calls["timing"] + 1),
        ),
    )

    main_module.main()

    assert calls == {"timing": 1, "run": 1}


def test_main_skips_timing_patch_when_disabled(monkeypatch):
    calls = {"timing": 0, "run": 0}

    async def _run() -> None:
        calls["run"] += 1

    monkeypatch.setattr(
        main_module,
        "discover",
        lambda: [{"name": "demo", "description": "", "run": _run}],
    )
    monkeypatch.setattr(main_module, "show_menu", lambda _backtests: 0)
    monkeypatch.setenv(main_module.ENABLE_TIMING_ENV, "0")
    monkeypatch.setitem(
        sys.modules,
        "backtests._shared._timing_test",
        SimpleNamespace(install_timing=lambda: calls.__setitem__("timing", 1)),
    )

    main_module.main()

    assert calls == {"timing": 0, "run": 1}


def test_show_menu_renders_folder_tree(capsys, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _prompt: "2")

    choice = main_module.show_menu(
        [
            {
                "name": "kalshi_breakout",
                "description": "Kalshi breakout",
                "relative_parts": ("kalshi_trade_tick", "kalshi_breakout.py"),
                "run": object(),
            },
            {
                "name": "kalshi_ema_crossover",
                "description": "Kalshi EMA",
                "relative_parts": ("kalshi_trade_tick", "kalshi_ema_crossover.py"),
                "run": object(),
            },
            {
                "name": "polymarket_pmxt_relay_breakout",
                "description": "PMXT breakout",
                "relative_parts": (
                    "polymarket_quote_tick",
                    "polymarket_pmxt_relay_breakout.py",
                ),
                "run": object(),
            },
        ],
    )

    rendered = _strip_ansi(capsys.readouterr().out)

    assert choice == 1
    assert "backtests/" in rendered
    assert "├── kalshi_trade_tick/" in rendered
    assert "│   ├── 1. kalshi_breakout.py — Kalshi breakout" in rendered
    assert "│   └── 2. kalshi_ema_crossover.py — Kalshi EMA" in rendered
    assert "└── polymarket_quote_tick/" in rendered
    assert "    └── 3. polymarket_pmxt_relay_breakout.py — PMXT breakout" in rendered
