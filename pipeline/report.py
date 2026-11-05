"""Step 5: put the results into one HTML page, refusing to build if its written findings are no longer true."""
import json
from pathlib import Path


def check_findings(quality, faults, signals):
    """The page states these three findings in words, so a rerun with different results must fail loudly."""
    assert not any(r["quarantined_rows"] for r in quality["rows"]), "finding 1 changed: rows were quarantined"
    assert not quality["gap_summary"], "finding 1 changed: minutes are missing"
    missed = [r["fault"] for r in faults["results"] if r["caught"] < r["injected"]]
    assert all(r["expected_reason"] is None for r in faults["results"] if r["fault"] in missed), \
        f"finding 2 changed: a check stopped working: {missed}"
    for name, s in signals["signals"].items():
        assert s["net_bp_per_hour"] < 0, f"finding 3 changed: {name} is profitable after costs"
        assert s["gross_ci_low_bp"] < 0 < s["gross_ci_high_bp"], f"finding 3 changed: {name} gross interval excludes 0"


def build(results_dir):
    results_dir = Path(results_dir)
    data = {name: json.loads((results_dir / f"{name}.json").read_text()) for name in ("ingest", "quality", "faults", "signals")}
    check_findings(data["quality"], data["faults"], data["signals"])
    template = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")
    assert template.count("/*DATA*/null") == 1
    # "<" is escaped so that no value in the JSON can close the script tag.
    page = template.replace("/*DATA*/null", json.dumps(data, separators=(",", ":")).replace("<", "\\u003c"))
    (results_dir / "dashboard.html").write_text(page, encoding="utf-8")
