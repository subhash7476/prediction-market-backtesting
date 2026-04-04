from __future__ import annotations

import re
import sys
from pathlib import Path
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
                "name": "kalshi_trade_tick_breakout",
                "description": "Kalshi breakout",
                "relative_parts": ("kalshi_trade_tick_breakout.py",),
                "run": object(),
            },
            {
                "name": "kalshi_trade_tick_ema_crossover",
                "description": "Kalshi EMA",
                "relative_parts": ("kalshi_trade_tick_ema_crossover.py",),
                "run": object(),
            },
            {
                "name": "polymarket_quote_tick_pmxt_breakout",
                "description": "PMXT breakout",
                "relative_parts": ("polymarket_quote_tick_pmxt_breakout.py",),
                "run": object(),
            },
        ],
    )

    rendered = _strip_ansi(capsys.readouterr().out)

    assert choice == 1
    assert "backtests/" in rendered
    assert "├── 1. kalshi_trade_tick_breakout.py — Kalshi breakout" in rendered
    assert "├── 2. kalshi_trade_tick_ema_crossover.py — Kalshi EMA" in rendered
    assert "└── 3. polymarket_quote_tick_pmxt_breakout.py — PMXT breakout" in rendered


def test_assign_shortcuts_prefers_unique_letters_and_avoids_quit_key():
    backtests = [
        {
            "name": "kalshi_trade_tick_breakout",
            "description": "Kalshi breakout",
            "relative_parts": ("kalshi_trade_tick_breakout.py",),
            "run": object(),
        },
        {
            "name": "polymarket_trade_tick_vwap_reversion",
            "description": "Polymarket VWAP",
            "relative_parts": ("polymarket_trade_tick_vwap_reversion.py",),
            "run": object(),
        },
        {
            "name": "polymarket_quote_tick_pmxt_ema_crossover",
            "description": "PMXT EMA",
            "relative_parts": ("polymarket_quote_tick_pmxt_ema_crossover.py",),
            "run": object(),
        },
    ]

    shortcuts = main_module._assign_shortcuts(backtests)

    assigned = [value for value in shortcuts.values() if value is not None]

    assert len(set(assigned)) == len(backtests)
    assert all(len(value) == 1 and value.isalpha() for value in assigned)
    assert "q" not in assigned
    assert "Q" not in assigned


def test_assign_shortcuts_leaves_overflow_entries_without_hotkeys():
    backtests = [
        {
            "name": f"demo_runner_{index}",
            "description": f"Demo {index}",
            "relative_parts": (f"demo_runner_{index}.py",),
            "run": object(),
        }
        for index in range(len(main_module.SHORTCUT_LETTERS) + 5)
    ]

    shortcuts = main_module._assign_shortcuts(backtests)
    assigned = [value for value in shortcuts.values() if value is not None]
    unassigned = [value for value in shortcuts.values() if value is None]

    assert len(assigned) == len(main_module.SHORTCUT_LETTERS)
    assert len(set(assigned)) == len(assigned)
    assert len(unassigned) == 5


def test_runner_preview_includes_command_and_spec(tmp_path: Path, monkeypatch):
    runner_path = tmp_path / "backtests" / "demo_runner.py"
    runner_path.parent.mkdir(parents=True)
    runner_path.write_text(
        'NAME = "demo_runner"\nDESCRIPTION = "Demo runner"\nDATA = object()\n',
        encoding="utf-8",
    )

    backtest = {
        "name": "demo_runner",
        "description": "Demo runner",
        "relative_parts": ("demo_runner.py",),
        "run": object(),
    }
    monkeypatch.setattr(main_module, "PROJECT_ROOT", tmp_path)

    preview = main_module._runner_preview(backtest)

    assert "backtests/demo_runner.py" in preview
    assert "uv run python backtests/demo_runner.py" in preview
    assert 'NAME = "demo_runner"' in preview


def test_discoverable_backtest_paths_stay_flat(tmp_path: Path) -> None:
    backtests_root = tmp_path / "backtests"
    (backtests_root / "_shared").mkdir(parents=True)
    (backtests_root / "private").mkdir()
    (backtests_root / "nested").mkdir()

    (backtests_root / "__init__.py").write_text("")
    (backtests_root / "_script_helpers.py").write_text("")
    (backtests_root / "kalshi_trade_tick_breakout.py").write_text("")
    (backtests_root / "private" / "local_runner.py").write_text("")
    (backtests_root / "private" / "_helper.py").write_text("")
    (backtests_root / "nested" / "should_not_show.py").write_text("")
    (backtests_root / "_shared" / "_trade_tick_ui.py").write_text("")

    discovered = [
        path.relative_to(backtests_root)
        for path in main_module._discoverable_backtest_paths(backtests_root)
    ]

    assert discovered == [
        Path("kalshi_trade_tick_breakout.py"),
        Path("private/local_runner.py"),
    ]
