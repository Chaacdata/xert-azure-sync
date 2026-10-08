"""
Load the stream data of every ride that has not been loaded yet.

For each ride in `activities` that is not in `stream_load_log`, this downloads
the per-second data from Xert and writes, in one transaction per ride:
    activity_stream           5 second buckets (elevation, power, MPA, ...)
    activity_power_curve      best average power per duration
    activity_power_histogram  seconds spent at each watt value

It can be stopped and started again at any time: finished rides are recorded in
`stream_load_log` (created automatically) and skipped next time. The same
command is what the daily sync will run, so it only ever does the new rides.

Needs in the same folder: xert_sync_azure.py, load_one_ride.py, load_power_summaries.py

Usage:
    export XERT_USERNAME=... XERT_PASSWORD=...
    export AZURE_SQL_SERVER=... AZURE_SQL_USER=... AZURE_SQL_PASSWORD=... AZURE_SQL_DATABASE=...
    python3 backfill_streams.py --limit 5        # try the 5 newest unloaded rides first
    python3 backfill_streams.py                  # everything that is left
    python3 backfill_streams.py --path <path>    # one ride, even if it was loaded before
"""

import argparse
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone

import requests

from load_one_ride import build_rows, insert_rows
from load_power_summaries import (
    compute_histogram,
    compute_power_curve,
    insert_batches,
    power_series,
)
from xert_sync_azure import (
    ACTIVITY_DETAIL_URL,
    check_env,
    get_access_token,
    get_connection,
    with_retries,
)

PAUSE_BETWEEN_RIDES_S = 0.5  # be polite to the Xert API
# A ride without stream data is only written off once it is this old; a ride that was
# just uploaded may simply not be processed by Xert yet and is tried again next run.
GIVE_UP_AFTER = timedelta(days=7)


def ensure_log_table(conn):
    cur = conn.cursor()
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='stream_load_log' AND xtype='U')
        CREATE TABLE stream_load_log (
            path      NVARCHAR(255) NOT NULL PRIMARY KEY,
            status    NVARCHAR(20)  NOT NULL,   -- loaded / no_stream / no_power / bad_data
            samples   INT NULL,
            loaded_at DATETIME2 NOT NULL
        )
    """)
    conn.commit()


def get_pending(conn, path=None, limit=None):
    cur = conn.cursor()
    if path:
        cur.execute("SELECT path, name, start_date FROM activities WHERE path = %s", (path,))
    else:
        cur.execute("""
            SELECT a.path, a.name, a.start_date
            FROM activities a LEFT JOIN stream_load_log l ON l.path = a.path
            WHERE l.path IS NULL
            ORDER BY a.start_date DESC
        """)
    rows = cur.fetchall()
    return rows[:limit] if limit else rows


def _fetch(session, token, path):
    resp = session.get(
        ACTIVITY_DETAIL_URL.format(path=path),
        headers={"Authorization": f"Bearer {token}"},
        params={"include_session_data": 1},
        timeout=180,  # a ride with per-second data is several MB
    )
    resp.raise_for_status()
    return resp.json()


def fetch_ride(session, token, path):
    return with_retries(_fetch, session, token, path, description=f"download {path}")


def write_log(conn, path, status, samples):
    cur = conn.cursor()
    cur.execute("""
        MERGE INTO stream_load_log AS t
        USING (SELECT %s AS path) AS s ON t.path = s.path
        WHEN MATCHED THEN UPDATE SET status = %s, samples = %s, loaded_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (path, status, samples, loaded_at)
            VALUES (%s, %s, %s, SYSUTCDATETIME());
    """, (path, status, samples, path, status, samples))


def process_ride(conn, data, path):
    """Write one ride. Returns (status, samples, stream_rows)."""
    session_data = data.get("session_data")
    if not isinstance(session_data, list) or not session_data:
        return "no_stream", 0, 0

    powers, _fallbacks, zero_filled = power_series(session_data)
    if zero_filled == len(powers):
        return "no_power", len(powers), 0

    try:
        stream_rows = build_rows(path, session_data)
    except ValueError:
        return "bad_data", len(powers), 0
    curve = compute_power_curve(powers)
    hist = compute_histogram(powers)
    if sum(s for _, s in hist) != len(powers):  # cannot happen, but never write a wrong histogram
        raise RuntimeError(f"histogram seconds do not match samples for {path}")

    cur = conn.cursor()
    for table in ("activity_stream", "activity_power_curve", "activity_power_histogram"):
        cur.execute(f"DELETE FROM {table} WHERE path = %s", (path,))
    insert_rows(conn, stream_rows)
    insert_batches(conn, "activity_power_curve", ["path", "duration_s", "best_power_w"],
                   [(path, d, b) for d, b in curve])
    insert_batches(conn, "activity_power_histogram", ["path", "power_w", "seconds"],
                   [(path, w, s) for w, s in hist])
    return "loaded", len(powers), len(stream_rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, help="only the N newest unloaded rides")
    parser.add_argument("--path", help="one specific ride, even if it was loaded before")
    args = parser.parse_args()

    check_env()
    conn = get_connection()
    ensure_log_table(conn)
    pending = get_pending(conn, args.path, args.limit)
    if args.path and not pending:
        sys.exit(f"{args.path} is not in the activities table.")
    total = len(pending)
    print(f"{total} ride(s) to load.")
    if not total:
        return

    session = requests.Session()
    token = get_access_token(session)
    started = time.time()
    counts = {}
    failures = []

    for i, (path, name, start_date) in enumerate(pending, 1):
        label = f"[{i}/{total}] {path} {str(name)[:40]!r}"
        t0 = time.time()
        for attempt in (1, 2):
            try:
                data = fetch_ride(session, token, path)
                status, samples, n_rows = process_ride(conn, data, path)
                too_new = (
                    status in ("no_stream", "no_power")
                    and start_date is not None
                    and datetime.now(timezone.utc).replace(tzinfo=None) - start_date < GIVE_UP_AFTER
                )
                if too_new:
                    conn.rollback()
                    print(f"{label}: {status}, ride is recent, will try again next run")
                    counts["retry_later"] = counts.get("retry_later", 0) + 1
                else:
                    write_log(conn, path, status, samples)
                    conn.commit()
                    counts[status] = counts.get(status, 0) + 1
                    extra = f", {n_rows} stream rows" if status == "loaded" else ""
                    print(f"{label}: {status}, {samples} samples{extra} ({time.time() - t0:.1f} s)")
                break
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                if attempt == 1:
                    print(f"{label}: problem ({e}); reconnecting and trying once more")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = get_connection()
                    continue
                print(f"{label}: FAILED ({e})")
                traceback.print_exc()
                failures.append(path)
                counts["failed"] = counts.get("failed", 0) + 1
        if i % 25 == 0:
            per_ride = (time.time() - started) / i
            print(f"  --- {i}/{total} done, about {per_ride * (total - i) / 60:.0f} min left ---")
        time.sleep(PAUSE_BETWEEN_RIDES_S)

    conn.close()
    print(f"\nFinished in {(time.time() - started) / 60:.1f} min: {counts}")
    if failures:
        print("Failed rides (run again to retry them):", ", ".join(failures))
        sys.exit(1)


if __name__ == "__main__":
    main()
