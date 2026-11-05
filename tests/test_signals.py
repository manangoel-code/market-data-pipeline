from datetime import date, datetime, timedelta

import numpy as np

from pipeline.signals import (block_bootstrap_ci, features, forward_returns, hourly_bars, portfolio,
                              quarter_starts, walk_forward)
from pipeline.validate import build
from tests.test_pipeline import DAY, row, write_day


def random_market(hours=3000, symbols=3, seed=1):
    rng = np.random.default_rng(seed)
    close = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, (hours, symbols)), axis=0))
    volume = rng.uniform(10, 100, (hours, symbols))
    return close, volume, volume * rng.uniform(0.2, 0.8, (hours, symbols))


def test_features_do_not_change_when_the_future_changes():
    close, volume, taker = random_market()
    cut = 2000
    before = features(close, volume, taker)
    close[cut + 1:] *= 3
    volume[cut + 1:] *= 7
    taker[cut + 1:] *= 2
    after = features(close, volume, taker)
    for name in before:
        np.testing.assert_array_equal(before[name][: cut + 1], after[name][: cut + 1])


def test_direction_ignores_returns_not_yet_known_at_the_quarter_start():
    close, volume, taker = random_market()
    signal = features(close, volume, taker)["past_1h_return"]
    forward = forward_returns(close)
    starts = [0, 500, 1000, 1500, 3000]
    _, before = walk_forward(signal, forward, starts)
    changed = forward.copy()
    changed[999:] = 100.0 * np.sign(np.nan_to_num(signal[999:]))  # Hour 999's return is only known at hour 1000.
    _, after = walk_forward(signal, changed, starts)
    assert before[0] == after[0]
    # The same change made one hour earlier is known by then, so it must be able to flip the direction.
    changed[998:] = -100.0 * np.sign(np.nan_to_num(signal[998:]))
    assert walk_forward(signal, changed, starts)[1][0] == -1.0


def test_no_positions_before_the_first_test_quarter():
    close, volume, taker = random_market()
    signal = features(close, volume, taker)["past_1h_return"]
    positions, _ = walk_forward(signal, forward_returns(close), [0, 500, 1000, 3000])
    assert not positions[:1000].any() and positions[1000:].any()


def test_planted_signal_is_found_and_faded_signal_is_flipped():
    rng = np.random.default_rng(2)
    signal = rng.normal(size=(3000, 3))
    forward = 0.001 * signal + 0.0001 * rng.normal(size=(3000, 3))
    starts = [0, 1000, 2000, 3000]
    positions, directions = walk_forward(signal, forward, starts, first_test=1)
    assert directions == [1.0, 1.0]
    gross, _, _ = portfolio(positions, forward)
    assert gross[1000:].mean() > 0
    assert walk_forward(-signal, forward, starts, first_test=1)[1] == [-1.0, -1.0]


def test_noise_has_no_significant_edge():
    rng = np.random.default_rng(3)
    forward = rng.normal(0, 0.01, (5000, 3))
    positions, _ = walk_forward(rng.normal(size=(5000, 3)), forward, [0, 1000, 5000], first_test=1)
    gross, _, _ = portfolio(positions, forward)
    low, high = block_bootstrap_ci(gross[1000:], 0.05)
    assert low < 0 < high


def test_costs_worked_by_hand():
    positions = np.array([[1.0], [-1.0], [-1.0], [1.0]])
    forward = np.array([[0.01], [0.02], [-0.01], [0.0]])
    gross, turnover, net = portfolio(positions, forward, cost_bp=10)
    np.testing.assert_allclose(gross, [0.01, -0.02, 0.01, 0.0])
    np.testing.assert_allclose(turnover, [1, 2, 0, 2])
    np.testing.assert_allclose(net, [0.009, -0.022, 0.01, -0.002])


def test_quarters_split_on_calendar_boundaries():
    first = datetime(2025, 3, 31, 22)
    hours = [first + timedelta(hours=i) for i in range(5)]
    assert quarter_starts(hours) == [0, 2, 5]


def test_hour_with_a_missing_minute_becomes_nan(tmp_path):
    write_day(tmp_path / "raw", [row(m) for m in range(120) if m != 70])
    build(tmp_path / "raw", tmp_path / "out", DAY, DAY)
    glob = (tmp_path / "out/clean/**/*.parquet").as_posix()
    _, close, _, _ = hourly_bars(glob, ["BTCUSDT"], date(2025, 1, 1), date(2025, 1, 1))
    assert close[0, 0] == 100 and np.isnan(close[1, 0]) and np.isnan(close[2, 0])
    assert len(close) == 24
