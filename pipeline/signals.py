"""Step 3: test whether simple hourly signals predict the next hour's return, after trading costs.

The direction of each signal (follow it or fade it) is chosen only from data before each test quarter.
"""
from datetime import datetime, timedelta

import duckdb
import numpy as np

COST_BP = 10.0  # Assumed cost per unit traded: Binance's standard 0.1% spot taker fee, ignoring the spread.
HOURS_PER_YEAR = 24 * 365
SIGNALS = 3
BLOCK_HOURS = 168
BOOTSTRAPS = 2000
FIRST_TEST_QUARTER = 2  # Two quarters of training before the first test.
FLASH_CRASH_DAY = "2025-10-10"  # The largest moves in the data, kept in but tested for influence.


def hourly_bars(clean_glob, symbols, start, end):
    """Close, volume and taker-buy volume per hour and symbol. An hour missing any minute becomes NaN."""
    table = duckdb.sql(f"""
        SELECT symbol, date_trunc('hour', minute) AS hour, count(*) AS minutes,
               arg_max(close, minute) AS close, sum(volume) AS volume, sum(taker_buy_volume) AS taker
        FROM read_parquet('{clean_glob}', hive_partitioning = true) GROUP BY ALL
    """).fetchall()
    first = datetime(start.year, start.month, start.day)
    count = int((datetime(end.year, end.month, end.day) - first).total_seconds() // 3600) + 24
    hours = [first + timedelta(hours=i) for i in range(count)]
    index = {hour: i for i, hour in enumerate(hours)}
    close, volume, taker = (np.full((count, len(symbols)), np.nan) for _ in range(3))
    for symbol, hour, minutes, c, v, t in table:
        if minutes == 60 and hour in index and symbol in symbols:
            i, j = index[hour], symbols.index(symbol)
            close[i, j], volume[i, j], taker[i, j] = c, v, t
    return hours, close, volume, taker


def features(close, volume, taker):
    """Each value at hour h uses data up to the end of hour h and nothing later."""
    past_1h, past_24h = np.full_like(close, np.nan), np.full_like(close, np.nan)
    past_1h[1:] = close[1:] / close[:-1] - 1
    past_24h[24:] = close[24:] / close[:-24] - 1
    with np.errstate(divide="ignore", invalid="ignore"):
        imbalance = 2 * taker / volume - 1
    return {"past_1h_return": past_1h, "past_24h_return": past_24h, "taker_buy_imbalance": imbalance}


def forward_returns(close):
    """Return from the close of hour h to the close of hour h + 1."""
    forward = np.full_like(close, np.nan)
    forward[:-1] = close[1:] / close[:-1] - 1
    return forward


def quarter_starts(hours):
    """Index of the first hour of every calendar quarter, plus the end of the data."""
    starts = [i for i, h in enumerate(hours) if i == 0 or (h.month - 1) // 3 != (hours[i - 1].month - 1) // 3]
    return starts + [len(hours)]


def walk_forward(signal, forward, starts, first_test=FIRST_TEST_QUARTER):
    """Positions for each test quarter, with the direction learned only from returns known by its start.

    Returns the positions (0 before the first test quarter) and the direction used per quarter.
    """
    signs = np.nan_to_num(np.sign(signal))
    returns = np.nan_to_num(forward)
    positions = np.zeros_like(signal)
    directions = []
    for q in range(first_test, len(starts) - 1):
        start, end = starts[q], starts[q + 1]
        # forward[start - 1] is only known at the close of hour start, so training stops one hour earlier.
        direction = 1.0 if np.sum(signs[: start - 1] * returns[: start - 1]) >= 0 else -1.0
        positions[start:end] = signs[start:end] * direction
        directions.append(direction)
    return positions, directions


def portfolio(positions, forward, cost_bp=COST_BP):
    """Equal-weight hourly gross return and turnover. Net return is gross - turnover * cost."""
    gross = np.mean(positions * np.nan_to_num(forward), axis=1)
    changes = np.abs(np.diff(positions, axis=0, prepend=np.zeros((1, positions.shape[1]))))
    turnover = np.mean(changes, axis=1)
    return gross, turnover, gross - turnover * cost_bp / 10000


def block_bootstrap_ci(series, alpha, seed=0, block=BLOCK_HOURS, reps=BOOTSTRAPS):
    """Interval for the mean, resampling week-long blocks because volatility clusters in time."""
    rng = np.random.default_rng(seed)
    blocks = int(np.ceil(len(series) / block))
    starts = rng.integers(0, len(series), size=(reps, blocks))
    offsets = np.arange(block)
    means = series[(starts[:, :, None] + offsets) % len(series)].reshape(reps, -1)[:, : len(series)].mean(axis=1)
    return float(np.quantile(means, alpha / 2)), float(np.quantile(means, 1 - alpha / 2))


def summarise(gross, turnover, net, alpha):
    low, high = block_bootstrap_ci(net, alpha)
    gross_low, gross_high = block_bootstrap_ci(gross, alpha)
    return {
        "gross_bp_per_hour": float(gross.mean() * 1e4),
        "gross_ci_low_bp": gross_low * 1e4,
        "gross_ci_high_bp": gross_high * 1e4,
        "net_bp_per_hour": float(net.mean() * 1e4),
        "net_ci_low_bp": low * 1e4,
        "net_ci_high_bp": high * 1e4,
        "net_annualised_sharpe": float(net.mean() / net.std() * np.sqrt(HOURS_PER_YEAR)) if net.std() else 0.0,
        "gross_annualised_sharpe": float(gross.mean() / gross.std() * np.sqrt(HOURS_PER_YEAR)) if gross.std() else 0.0,
        "turnover_per_hour": float(turnover.mean()),
        "break_even_cost_bp": float(gross.mean() / turnover.mean() * 1e4) if turnover.mean() else None,
    }


def run(clean_glob, symbols, start, end):
    hours, close, volume, taker = hourly_bars(clean_glob, symbols, start, end)
    forward = forward_returns(close)
    starts = quarter_starts(hours)
    test_start = starts[FIRST_TEST_QUARTER]
    alpha = 0.05 / SIGNALS  # Bonferroni: three signals get three chances to look good.
    keep = np.array([h.strftime("%Y-%m-%d") != FLASH_CRASH_DAY for h in hours[test_start:]])
    # Daily points for the chart, plus the final hour so that its lines end at the same totals as the table.
    daily = sorted({*range(0, len(hours) - test_start, 24), len(hours) - test_start - 1})
    report = {
        "test_period": [str(hours[test_start]), str(hours[-1])],
        "test_hours": len(hours) - test_start,
        "unusable_hours": int(np.isnan(close).sum()),
        "cost_bp_per_unit": COST_BP,
        "confidence_level": 1 - alpha,
        "curve_dates": [hours[test_start + i].strftime("%Y-%m-%d") for i in daily],
        "signals": {},
    }
    for name, signal in features(close, volume, taker).items():
        positions, directions = walk_forward(signal, forward, starts)
        gross, turnover, net = (s[test_start:] for s in portfolio(positions, forward))
        entry = summarise(gross, turnover, net, alpha)
        entry["directions"] = directions
        entry["net_without_flash_crash_bp"] = float(net[keep].mean() * 1e4)
        entry["gross_curve"] = [round(float(x), 5) for x in np.cumsum(gross)[list(daily)]]
        entry["net_curve"] = [round(float(x), 5) for x in np.cumsum(net)[list(daily)]]
        report["signals"][name] = entry
    hold = np.mean(np.nan_to_num(forward), axis=1)[test_start:]
    report["buy_and_hold"] = {
        "bp_per_hour": float(hold.mean() * 1e4),
        "annualised_sharpe": float(hold.mean() / hold.std() * np.sqrt(HOURS_PER_YEAR)),
        "curve": [round(float(x), 5) for x in np.cumsum(hold)[list(daily)]],
    }
    return report
