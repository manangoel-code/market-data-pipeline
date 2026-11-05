import hashlib
import io
import zipfile
from datetime import date

import duckdb
import pytest

from pipeline.ingest import sync_day
from pipeline.validate import build

DAY = date(2025, 1, 1)
DAY_MS = 1735689600000  # 2025-01-01 00:00 UTC, the first day the archive uses microseconds


def row(minute, base=DAY_MS, scale=1000, o=100, h=101, l=99, c=100, v=10, trades=5, taker=4):
    t = (base + minute * 60000) * scale
    return f"{t},{o},{h},{l},{c},{v},{t + 59999},1000,{trades},{taker},400,0"


def write_day(raw, lines, symbol="BTCUSDT", day=DAY):
    folder = raw / symbol
    folder.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(folder / f"{symbol}-1m-{day}.zip", "w") as archive:
        archive.writestr(f"{symbol}-1m-{day}.csv", "\n".join(lines) + "\n")


def run(tmp_path, lines, start=DAY, end=DAY):
    write_day(tmp_path / "raw", lines)
    return build(tmp_path / "raw", tmp_path / "out", start, end)


def clean_table(tmp_path):
    return duckdb.sql(f"""SELECT * FROM read_parquet('{(tmp_path / "out/clean/**/*.parquet").as_posix()}',
                          hive_partitioning = true) ORDER BY minute""").fetchall()


def reasons(report):
    return {r["reason"]: r["n"] for r in report["quarantine_reasons"]}


def test_both_timestamp_units_give_the_same_minute(tmp_path):
    write_day(tmp_path / "raw", [row(0, base=1735603200000, scale=1)], day=date(2024, 12, 31))
    write_day(tmp_path / "raw", [row(0)], day=DAY)
    report = build(tmp_path / "raw", tmp_path / "out", date(2024, 12, 31), DAY)
    assert report["rows"][0]["millisecond_rows"] == 1 and report["rows"][0]["microsecond_rows"] == 1
    minutes = [str(r[0]) for r in duckdb.sql(f"""SELECT minute FROM read_parquet(
        '{(tmp_path / "out/clean/**/*.parquet").as_posix()}', hive_partitioning = true) ORDER BY minute""").fetchall()]
    assert minutes == ["2024-12-31 00:00:00", "2025-01-01 00:00:00"]


@pytest.mark.parametrize("bad, reason", [
    ("not,a,row", "unparsable"),
    (row(1, o="nan"), "unparsable"),
    (row(1, c=-1, l=-2), "non_positive_price"),
    (row(1, h=99.5), "ohlc_inconsistent"),
    (row(1, l=100.5), "ohlc_inconsistent"),
    (row(1, v=-1, taker=-2), "negative_volume"),
    (row(1, v=1, taker=4), "taker_exceeds_volume"),
    (row(1441), "outside_file_date"),
    (str((DAY_MS + 60500) * 1000) + ",100,101,99,100,10,0,1000,5,4,400,0", "not_on_minute_boundary"),
])
def test_each_bad_row_is_quarantined_with_its_reason(tmp_path, bad, reason):
    report = run(tmp_path, [row(0), bad])
    assert reasons(report) == {reason: 1}
    assert report["rows"][0]["clean_rows"] == 1


def test_exact_duplicate_keeps_one_row(tmp_path):
    report = run(tmp_path, [row(0), row(0)])
    assert report["rows"][0]["exact_duplicates"] == 1
    assert len(clean_table(tmp_path)) == 1


def test_conflicting_duplicate_keeps_neither_and_leaves_a_gap(tmp_path):
    report = run(tmp_path, [row(0), row(0, c=100.5), row(1)])
    assert reasons(report) == {"conflicting_duplicate": 2}
    assert "2025-01-01 00:00" in [g["first_missing"] for g in report["longest_gaps"]]


def test_gaps_are_found_as_runs(tmp_path):
    present = [m for m in range(1440) if not 100 <= m < 103 and m != 500]
    report = run(tmp_path, [row(m) for m in present])
    assert report["gap_summary"] == [{"symbol": "BTCUSDT", "gaps": 2, "missing_minutes": 4, "longest": 3}]
    assert report["longest_gaps"][0]["first_missing"] == "2025-01-01 01:40"


def test_every_raw_row_is_accounted_for(tmp_path):
    report = run(tmp_path, [row(0), row(0), row(1, h=1), "junk", row(2), row(2, c=100.5)])
    r = report["rows"][0]
    assert r["raw_rows"] == 6
    assert r["raw_rows"] == r["clean_rows"] + r["exact_duplicates"] + r["quarantined_rows"]


def test_rerun_gives_identical_clean_data(tmp_path):
    run(tmp_path, [row(0), row(1)])
    first = clean_table(tmp_path)
    build(tmp_path / "raw", tmp_path / "out", DAY, DAY)
    assert clean_table(tmp_path) == first


def zip_bytes():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("x.csv", row(0))
    return buffer.getvalue()


def serve(data, published):
    return lambda url: published if url.endswith(".CHECKSUM") else data


def test_good_file_downloads_then_is_cached_without_fetching(tmp_path):
    data = zip_bytes()
    checksum = f"{hashlib.sha256(data).hexdigest()}  x.zip".encode()
    assert sync_day(tmp_path, "BTCUSDT", DAY, serve(data, checksum)) == "downloaded"

    def no_network(url):
        raise AssertionError("should not fetch a verified file")

    assert sync_day(tmp_path, "BTCUSDT", DAY, no_network) == "cached"


def test_wrong_checksum_never_reaches_disk(tmp_path):
    status = sync_day(tmp_path, "BTCUSDT", DAY, serve(zip_bytes(), b"0" * 64 + b"  x.zip"))
    assert status == "checksum_failed"
    assert not list(tmp_path.rglob("*.zip"))


def test_damaged_file_on_disk_is_replaced(tmp_path):
    data = zip_bytes()
    checksum = f"{hashlib.sha256(data).hexdigest()}  x.zip".encode()
    sync_day(tmp_path, "BTCUSDT", DAY, serve(data, checksum))
    path = next(tmp_path.rglob("*.zip"))
    path.write_bytes(b"damaged")
    assert sync_day(tmp_path, "BTCUSDT", DAY, serve(data, checksum)) == "downloaded"
    assert path.read_bytes() == data


def test_missing_file_is_reported(tmp_path):
    assert sync_day(tmp_path, "BTCUSDT", DAY, lambda url: None) == "missing"


def test_report_refuses_to_build_when_a_written_finding_is_false():
    from pipeline.report import check_findings
    good = {"quality": {"rows": [{"quarantined_rows": 0}], "gap_summary": []},
            "faults": {"results": [{"fault": "a", "injected": 20, "caught": 20, "expected_reason": "x"},
                                   {"fault": "b", "injected": 20, "caught": 0, "expected_reason": None}]},
            "signals": {"signals": {"s": {"net_bp_per_hour": -1, "gross_ci_low_bp": -1, "gross_ci_high_bp": 1}}}}
    check_findings(good["quality"], good["faults"], good["signals"])
    broken = {"results": [{"fault": "a", "injected": 20, "caught": 19, "expected_reason": "x"}]}
    with pytest.raises(AssertionError):
        check_findings(good["quality"], broken, good["signals"])
    profit = {"signals": {"s": {"net_bp_per_hour": 1, "gross_ci_low_bp": -1, "gross_ci_high_bp": 1}}}
    with pytest.raises(AssertionError):
        check_findings(good["quality"], good["faults"], profit)
