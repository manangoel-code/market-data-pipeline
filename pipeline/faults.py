"""Step 4: break a copy of real data on purpose and measure which faults the pipeline catches.

The real archive turned out clean, so this is the evidence that the checks work. It also shows what
they miss: a wrong price that still looks internally consistent passes every rule.
"""
import random
import shutil
import zipfile
from datetime import date
from pathlib import Path

from pipeline.validate import build

DAY = date(2026, 3, 2)
FAULTS_PER_TYPE = 20


def _set(fields, **changes):
    names = {"open": 1, "high": 2, "low": 3, "close": 4, "volume": 5, "taker": 9}
    for name, value in changes.items():
        fields[names[name]] = str(value)
    return ",".join(fields)


# Each fault rewrites one row, and names the quarantine reason that should catch it.
# None means no rule is expected to catch it, so it is measured as a known blind spot.
ROW_FAULTS = {
    "text in a number column": ("unparsable", lambda f: _set(f, close="error")),
    "row cut short": ("unparsable", lambda f: ",".join(f[:3])),
    "timestamp off the minute": ("not_on_minute_boundary", lambda f: ",".join([str(int(f[0]) + 500), *f[1:]])),
    "row from another day": ("outside_file_date", lambda f: ",".join([str(int(f[0]) + 172800000000), *f[1:]])),
    "zero price": ("non_positive_price", lambda f: _set(f, open=0, low=0)),
    "high below close": ("ohlc_inconsistent", lambda f: _set(f, high=float(f[4]) * 0.9)),
    "negative volume": ("negative_volume", lambda f: _set(f, volume=-1, taker=-2)),
    "taker volume above total": ("taker_exceeds_volume", lambda f: _set(f, taker=float(f[5]) * 2 + 1)),
    "all prices 2% too high": (None, lambda f: _set(f, **{k: float(f[i]) * 1.02 for k, i in
                                                           (("open", 1), ("high", 2), ("low", 3), ("close", 4))})),
    "volume 10 times too high": (None, lambda f: _set(f, volume=float(f[5]) * 10, taker=float(f[9]) * 10)),
}


def _lines(raw_dir, symbol):
    path = Path(raw_dir) / symbol / f"{symbol}-1m-{DAY}.zip"
    with zipfile.ZipFile(path) as archive:
        return archive.read(archive.namelist()[0]).decode().splitlines()


def _check(work_dir, symbol, lines):
    """Run the real pipeline on one altered day and return its report."""
    raw, out = work_dir / "raw", work_dir / "out"
    for folder in (raw, out):
        shutil.rmtree(folder, ignore_errors=True)
    (raw / symbol).mkdir(parents=True)
    with zipfile.ZipFile(raw / symbol / f"{symbol}-1m-{DAY}.zip", "w") as archive:
        archive.writestr(f"{symbol}-1m-{DAY}.csv", "\n".join(lines) + "\n")
    return build(raw, out, DAY, DAY)


def run(raw_dir, work_dir, symbol="BTCUSDT", seed=0):
    work_dir = Path(work_dir)
    rng = random.Random(seed)
    original = _lines(raw_dir, symbol)
    results = []

    def record(name, expected, caught):
        results.append({"fault": name, "injected": FAULTS_PER_TYPE, "caught": caught, "expected_reason": expected})

    for name, (reason, change) in ROW_FAULTS.items():
        lines = list(original)
        for i in rng.sample(range(len(lines)), FAULTS_PER_TYPE):
            lines[i] = change(lines[i].split(","))
        report = _check(work_dir, symbol, lines)
        caught = sum(r["n"] for r in report["quarantine_reasons"] if r["reason"] == reason)
        record(name, reason, caught)

    chosen = rng.sample(range(len(original)), FAULTS_PER_TYPE)

    lines = list(original) + [original[i] for i in chosen]
    report = _check(work_dir, symbol, lines)
    record("exact repeat of a row", "exact_duplicate", report["rows"][0]["exact_duplicates"])

    lines = list(original) + [_set(original[i].split(","), volume=float(original[i].split(",")[5]) + 1) for i in chosen]
    report = _check(work_dir, symbol, lines)
    caught = sum(r["n"] for r in report["quarantine_reasons"] if r["reason"] == "conflicting_duplicate") // 2
    record("two versions of one minute", "conflicting_duplicate", caught)

    lines = [line for i, line in enumerate(original) if i not in set(chosen)]
    report = _check(work_dir, symbol, lines)
    missing = report["gap_summary"][0]["missing_minutes"] if report["gap_summary"] else 0
    record("row silently dropped", "gap", missing)

    shutil.rmtree(work_dir, ignore_errors=True)
    return {"symbol": symbol, "day": str(DAY), "rows_in_day": len(original), "results": results}
