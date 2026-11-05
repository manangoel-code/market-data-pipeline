"""Step 1: download daily 1-minute klines from Binance's public archive and verify each file.

Every zip is checked against its published SHA-256 on every run, so a rerun skips good files and
replaces damaged ones. A file only appears under its final name once it has passed the check.
"""
import hashlib
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

BASE_URL = "https://data.binance.vision/data/spot/daily/klines"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
# Starts six months before the archive's timestamps change from milliseconds to microseconds.
START, END = date(2024, 7, 1), date(2026, 9, 30)


def fetch(url):
    """The bytes at url, or None if the server says the file does not exist."""
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            if attempt == 2:
                raise
        except OSError:
            if attempt == 2:
                raise
        time.sleep(2**attempt)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def sync_day(raw_dir, symbol, day, fetch=fetch):
    """Make sure one verified file is on disk. Returns cached, downloaded, missing or checksum_failed."""
    name = f"{symbol}-1m-{day}.zip"
    url = f"{BASE_URL}/{symbol}/1m/{name}"
    folder = Path(raw_dir) / symbol
    path, checksum_path = folder / name, folder / f"{name}.CHECKSUM"

    if path.exists() and checksum_path.exists():
        if sha256(path.read_bytes()) == checksum_path.read_text().split()[0]:
            return "cached"

    data, checksum = fetch(url), fetch(url + ".CHECKSUM")
    if data is None or checksum is None:
        return "missing"
    checksum = checksum.decode()
    if sha256(data) != checksum.split()[0]:
        return "checksum_failed"

    folder.mkdir(parents=True, exist_ok=True)
    for target, content in ((checksum_path, checksum.encode()), (path, data)):
        temporary = target.with_name(target.name + ".tmp")
        temporary.write_bytes(content)
        os.replace(temporary, target)
    return "downloaded"


def sync_all(raw_dir, symbols=SYMBOLS, start=START, end=END, workers=8):
    """Sync every symbol and day. Returns the status counts and every file that needs attention."""
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    jobs = [(symbol, day) for symbol in symbols for day in days]

    def run(job):
        try:
            return sync_day(raw_dir, *job)
        except OSError as error:
            return f"error: {error}"

    with ThreadPoolExecutor(workers) as pool:
        statuses = list(pool.map(run, jobs))

    counts, problems = {}, []
    for (symbol, day), status in zip(jobs, statuses):
        kind = status.split(":")[0]
        counts[kind] = counts.get(kind, 0) + 1
        if kind not in ("cached", "downloaded"):
            problems.append({"symbol": symbol, "date": str(day), "status": status})
    return {"files_expected": len(jobs), "counts": counts, "problems": problems}
