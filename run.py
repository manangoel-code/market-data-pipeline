"""Runs the pipeline: python run.py [ingest] [validate] [signals] [faults] [report]. With no arguments, runs all of them."""
import json
import sys
from pathlib import Path

from pipeline import faults, report, signals
from pipeline.ingest import END, START, SYMBOLS, sync_all
from pipeline.validate import build

ROOT = Path(__file__).parent
RAW, OUT, RESULTS = ROOT / "data/raw", ROOT / "data/processed", ROOT / "results"


def save(name, report):
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / name).write_text(json.dumps(report, indent=1, default=str) + "\n")


steps = sys.argv[1:] or ["ingest", "validate", "signals", "faults", "report"]
if "ingest" in steps:
    result = sync_all(RAW)
    save("ingest.json", result)
    print("ingest:", result["counts"], "of", result["files_expected"])
if "validate" in steps:
    result = build(RAW, OUT, START, END)
    save("quality.json", result)
    print("validate:", result["rows"])
if "signals" in steps:
    result = signals.run((OUT / "clean/**/*.parquet").as_posix(), SYMBOLS, START, END)
    save("signals.json", result)
    print("signals: test period", result["test_period"])
if "faults" in steps:
    result = faults.run(RAW, ROOT / "data/_faults")
    save("faults.json", result)
    print("faults: done")
if "report" in steps:
    report.build(RESULTS)
    print("report: results/dashboard.html")
