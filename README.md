# Market data pipeline

A batch pipeline for 27 months of public 1-minute market data from Binance (5 symbols, July 2024 to September 2026). It
checks every file and row, stores clean Parquet, and tests whether simple signals beat trading costs. To see the results,
open `results/dashboard.html` in a browser.

## What it does

| Step | Module | What happens |
|---|---|---|
| Download | `pipeline/ingest.py` | Checks each daily zip against its published SHA-256. Reruns skip good files and replace damaged ones. |
| Validate | `pipeline/validate.py` | Reads everything as text with DuckDB, applies row rules, and sends failures to a quarantine file with a reason. Writes clean Parquet by symbol and month. |
| Fault test | `pipeline/faults.py` | Adds 13 kinds of damage to a copy of real data and counts what the checks catch. |
| Signal test | `pipeline/signals.py` | Walk-forward test of 3 hourly signals with costs and block-bootstrap intervals. |
| Report | `pipeline/report.py` | Builds the results page, and refuses to build if its written findings no longer match the data. |

## Results

All 4,110 files matched their checksums, and all 5,918,400 rows passed every rule with no minute missing. The archive does
change timestamps from milliseconds to microseconds on 1 January 2025, so the pipeline converts both to one unit.

With no real errors to find, the rules were tested by adding damage on purpose. They fully caught 11 of 13 fault types. Two
got through: all prices 2% too high, and volume 10 times too high. A wrong value that stays internally consistent passes
every row rule, and catching it would need a second data source.

None of the 3 signals beat costs. The test ran from January 2025 to September 2026, with each signal's direction chosen only
from earlier data:

| Signal | Gross bp per hour (98.3% interval) | Net bp per hour (98.3% interval) |
|---|---|---|
| Past 1-hour return | -0.28 (-1.09 to +0.57) | -10.57 (-11.30 to -9.78) |
| Past 24-hour return | -0.26 (-1.26 to +0.65) | -2.13 (-3.08 to -1.28) |
| Taker buy imbalance | -0.01 (-0.71 to +0.66) | -8.57 (-9.23 to -7.90) |

A basis point (bp) is 0.01%. The 98.3% level allows for testing 3 signals (Bonferroni correction).

## Limits

- Costs are assumed at 10 bp per unit traded, based on a standard taker fee. Bid-ask spread and market impact are left out,
  and every trade is assumed to fill at the hour's closing price.
- It covers 5 large symbols on one exchange over one period, so it says nothing about other signals, markets or horizons.
- Prices were not compared with another exchange, including the largest moves on 10 October 2025. They are flagged and kept.
- The fault test uses one day of BTCUSDT, so its counts show that the rules fire. They do not estimate real-world error rates.

## Run it

You need Python 3.12. The download is about 265 MB.

```bash
python -m venv .venv
```

```bash
.venv/Scripts/python -m pip install -r requirements.txt
```

```bash
.venv/Scripts/python -m pytest
```

```bash
.venv/Scripts/python run.py
```

On Linux or macOS, use `.venv/bin/python`. A single step can be run by name, for example `run.py validate`.
