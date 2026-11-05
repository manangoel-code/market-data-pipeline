"""Step 2: turn verified raw files into clean Parquet, keeping every rejected row and its reason.

Every raw row ends up in exactly one place: clean, quarantine, or counted as an exact duplicate.
"""
import shutil
import zipfile
from pathlib import Path

import duckdb

COLUMNS = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume",
           "trades", "taker_buy_volume", "taker_buy_quote_volume", "ignore"]

# Whole-number cutoff between the archive's two timestamp units: milliseconds until 2024,
# microseconds from 1 Jan 2025. 1e14 ms is the year 5138 and 1e14 us is 1973, so it can't misread either.
MICROSECOND_CUTOFF = 100_000_000_000_000


def extract(raw_dir, work_dir):
    """Unzip every raw file into one folder, named so each row can be traced to its source file."""
    work_dir.mkdir(parents=True)
    for path in sorted(Path(raw_dir).glob("*/*.zip")):
        with zipfile.ZipFile(path) as archive:
            for member in archive.namelist():
                (work_dir / member).write_bytes(archive.read(member))


def load_and_classify(con, csv_glob):
    """Read everything as text, so a malformed row becomes a quarantined row and not a crash."""
    columns = ", ".join(f"'{c}': 'VARCHAR'" for c in COLUMNS)
    con.execute(f"""
        CREATE TABLE parsed AS
        WITH raw AS (
            SELECT *, regexp_extract(filename, '([A-Z0-9]+)-1m-', 1) AS symbol,
                   TRY_CAST(regexp_extract(filename, '-1m-([0-9-]+)\\.csv', 1) AS DATE) AS file_date
            FROM read_csv('{csv_glob}', header = false, auto_detect = false, delim = ',',
                          columns = {{{columns}}}, filename = true, null_padding = true,
                          strict_mode = false, all_varchar = true)
        ), typed AS (
            SELECT symbol, file_date, open_time AS raw_open_time,
                   TRY_CAST(open_time AS BIGINT) AS t,
                   TRY_CAST(open AS DOUBLE) AS open, TRY_CAST(high AS DOUBLE) AS high,
                   TRY_CAST(low AS DOUBLE) AS low, TRY_CAST(close AS DOUBLE) AS close,
                   TRY_CAST(volume AS DOUBLE) AS volume, TRY_CAST(trades AS BIGINT) AS trades,
                   TRY_CAST(taker_buy_volume AS DOUBLE) AS taker_buy_volume
            FROM raw
        ), scaled AS (
            SELECT *, t >= {MICROSECOND_CUTOFF} AS in_microseconds,
                   CASE WHEN t >= {MICROSECOND_CUTOFF} THEN t // 1000 ELSE t END AS ts_ms,
                   CASE WHEN t >= {MICROSECOND_CUTOFF} THEN t % 60000000 ELSE t % 60000 END AS off_minute
            FROM typed
        )
        SELECT *, make_timestamp(ts_ms * 1000) AS minute,
            CASE
                WHEN t IS NULL OR open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
                     OR volume IS NULL OR trades IS NULL OR taker_buy_volume IS NULL
                     OR NOT (isfinite(open) AND isfinite(high) AND isfinite(low) AND isfinite(close)
                             AND isfinite(volume) AND isfinite(taker_buy_volume))
                     OR t < 0 THEN 'unparsable'
                WHEN off_minute <> 0 THEN 'not_on_minute_boundary'
                WHEN CAST(make_timestamp(ts_ms * 1000) AS DATE) <> file_date THEN 'outside_file_date'
                WHEN LEAST(open, high, low, close) <= 0 THEN 'non_positive_price'
                WHEN high < GREATEST(open, close, low) OR low > LEAST(open, close, high) THEN 'ohlc_inconsistent'
                WHEN volume < 0 OR taker_buy_volume < 0 OR trades < 0 THEN 'negative_volume'
                WHEN taker_buy_volume > volume * (1 + 1e-9) THEN 'taker_exceeds_volume'
            END AS reason
        FROM scaled
    """)
    # Exact repeats are harmless, so one is kept. If two rows disagree about a minute, neither can be trusted.
    con.execute("""
        CREATE TABLE classified AS
        SELECT *, CASE
                WHEN reason IS NOT NULL THEN reason
                WHEN versions > 1 THEN 'conflicting_duplicate'
                WHEN copy > 1 THEN 'exact_duplicate'
            END AS outcome
        FROM (
            SELECT *,
                CASE WHEN reason IS NULL THEN row_number() OVER w END AS copy,
                CASE WHEN reason IS NULL THEN
                    (min(hash(open, high, low, close, volume, trades, taker_buy_volume)) OVER w
                     <> max(hash(open, high, low, close, volume, trades, taker_buy_volume)) OVER w)::INT + 1
                END AS versions
            FROM parsed
            WINDOW w AS (PARTITION BY symbol, CASE WHEN reason IS NULL THEN ts_ms END, reason IS NULL)
        )
    """)


def write_outputs(con, out_dir):
    """Build new outputs beside the old ones, then swap, so a failed run leaves the last good data."""
    clean, quarantine = out_dir / "clean", out_dir / "quarantine.parquet"
    new_clean, new_quarantine = out_dir / "_clean_new", out_dir / "_quarantine_new.parquet"
    shutil.rmtree(new_clean, ignore_errors=True)
    con.execute(f"""
        COPY (
            SELECT symbol, minute, open, high, low, close, volume, trades, taker_buy_volume,
                   strftime(minute, '%Y-%m') AS month
            FROM classified WHERE outcome IS NULL ORDER BY symbol, minute
        ) TO '{new_clean.as_posix()}' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (symbol, month))
    """)
    con.execute(f"""
        COPY (
            SELECT symbol, file_date, raw_open_time, open, high, low, close, volume, outcome AS reason
            FROM classified WHERE outcome IS NOT NULL AND outcome <> 'exact_duplicate'
        ) TO '{new_quarantine.as_posix()}' (FORMAT parquet, COMPRESSION zstd)
    """)
    shutil.rmtree(clean, ignore_errors=True)
    new_clean.rename(clean)
    new_quarantine.replace(quarantine)


def rows(con, sql):
    result = con.execute(sql)
    names = [d[0] for d in result.description]
    return [dict(zip(names, row)) for row in result.fetchall()]


def quality_report(con, start, end):
    """Row accounting, gaps against a complete minute grid, and rows flagged but kept for review."""
    con.execute(f"""
        CREATE TABLE grid AS
        SELECT s.symbol, g.minute FROM (SELECT DISTINCT symbol FROM classified) s,
        generate_series(TIMESTAMP '{start} 00:00:00', TIMESTAMP '{end} 23:59:00', INTERVAL 1 MINUTE) g(minute)
    """)
    con.execute("""
        CREATE TABLE missing AS
        SELECT g.symbol, g.minute FROM grid g
        ANTI JOIN (SELECT symbol, minute FROM classified WHERE outcome IS NULL) c USING (symbol, minute)
    """)
    con.execute("""
        CREATE TABLE gaps AS
        SELECT symbol, min(minute) AS first_missing, max(minute) AS last_missing, count(*) AS minutes
        FROM (SELECT symbol, minute, minute - row_number() OVER (PARTITION BY symbol ORDER BY minute)
                     * INTERVAL 1 MINUTE AS run FROM missing)
        GROUP BY symbol, run
    """)
    report = {
        "rows": rows(con, """
            SELECT symbol, count(*) AS raw_rows, count(*) FILTER (outcome IS NULL) AS clean_rows,
                   count(*) FILTER (outcome = 'exact_duplicate') AS exact_duplicates,
                   count(*) FILTER (outcome IS NOT NULL AND outcome <> 'exact_duplicate') AS quarantined_rows,
                   count(*) FILTER (in_microseconds) AS microsecond_rows,
                   count(*) FILTER (NOT in_microseconds) AS millisecond_rows
            FROM classified GROUP BY symbol ORDER BY symbol"""),
        "quarantine_reasons": rows(con, """
            SELECT symbol, outcome AS reason, count(*) AS n FROM classified
            WHERE outcome IS NOT NULL AND outcome <> 'exact_duplicate'
            GROUP BY ALL ORDER BY symbol, reason"""),
        "completeness": rows(con, """
            SELECT g.symbol, strftime(g.minute, '%Y-%m') AS month, count(*) AS expected,
                   count(*) - count(m.minute) AS present
            FROM grid g LEFT JOIN missing m USING (symbol, minute)
            GROUP BY ALL ORDER BY symbol, month"""),
        "gap_summary": rows(con, """
            SELECT symbol, count(*) AS gaps, sum(minutes) AS missing_minutes, max(minutes) AS longest
            FROM gaps GROUP BY symbol ORDER BY symbol"""),
        "longest_gaps": rows(con, """
            SELECT symbol, strftime(first_missing, '%Y-%m-%d %H:%M') AS first_missing,
                   strftime(last_missing, '%Y-%m-%d %H:%M') AS last_missing, minutes
            FROM gaps ORDER BY minutes DESC, symbol, first_missing LIMIT 15"""),
        "flags": rows(con, """
            WITH clean AS (
                SELECT symbol, minute, volume, close,
                       close / lag(close) OVER (PARTITION BY symbol ORDER BY minute) - 1 AS change
                FROM classified WHERE outcome IS NULL
            )
            SELECT symbol, count(*) FILTER (volume = 0) AS zero_volume_minutes,
                   count(*) FILTER (abs(change) > 0.05) AS moves_over_5_percent
            FROM clean GROUP BY symbol ORDER BY symbol"""),
        "largest_moves": rows(con, """
            WITH clean AS (
                SELECT symbol, minute,
                       close / lag(close) OVER (PARTITION BY symbol ORDER BY minute) - 1 AS change
                FROM classified WHERE outcome IS NULL
            )
            SELECT symbol, strftime(minute, '%Y-%m-%d %H:%M') AS minute, round(change * 100, 2) AS percent
            FROM clean WHERE change IS NOT NULL ORDER BY abs(change) DESC LIMIT 10"""),
    }
    total = sum(r["raw_rows"] for r in report["rows"])
    accounted = sum(r["clean_rows"] + r["exact_duplicates"] + r["quarantined_rows"] for r in report["rows"])
    assert total == accounted, f"rows lost: {total} raw, {accounted} accounted for"
    return report


def build(raw_dir, out_dir, start, end):
    """Run the whole step and return its quality report."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    work_dir = out_dir / "_csv"
    shutil.rmtree(work_dir, ignore_errors=True)
    try:
        extract(raw_dir, work_dir)
        con = duckdb.connect()
        load_and_classify(con, (work_dir / "*.csv").as_posix())
        write_outputs(con, out_dir)
        report = quality_report(con, start, end)
        con.close()
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
    return report
