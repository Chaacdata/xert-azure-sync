"""
Compute the power duration curve and the power histogram of ONE ride and load
them into Azure SQL (tables: activity_power_curve, activity_power_histogram).

Reads the JSON saved by inspect_ride_stream.py (ride_<path>.json), so nothing
is fetched from Xert. Running it again replaces that ride's rows.

Both are computed from the raw 1 s power (p_raw), not the smoothed `power`
column, because smoothing flattens the short-duration peaks.

Needs load_one_ride.py in the same folder (for the Azure connection helper).

Usage:
    export AZURE_SQL_SERVER="chaac-xert.database.windows.net"
    export AZURE_SQL_USER="chaac"
    export AZURE_SQL_PASSWORD="your_password"
    export AZURE_SQL_DATABASE="xertdb"
    python3 load_power_summaries.py ride_<path>.json
"""

import json
import os
import sys
from collections import Counter
from itertools import accumulate
from operator import sub

from load_one_ride import connect, fmt

# Durations (seconds) stored per ride, roughly log-spaced from 1 s to 12 h.
# A duration longer than the ride itself is skipped for that ride.
DURATIONS_S = [
    1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 25, 30, 40, 45, 60, 75, 90, 120, 150,
    180, 240, 300, 360, 420, 480, 600, 720, 900, 1200, 1500, 1800, 2400, 3000,
    3600, 4500, 5400, 7200, 9000, 10800, 14400, 18000, 21600, 28800, 36000,
    43200,
]

BATCH_SIZE = 250


def power_series(session_data):
    """Raw power per sample. Falls back to the smoothed value, then to 0."""
    series = []
    fallbacks = 0
    zero_filled = 0
    for r in session_data:
        p = r.get("p_raw")
        if p is None:
            p = r.get("power")
            if p is not None:
                fallbacks += 1
        if p is None:
            p = 0.0
            zero_filled += 1
        series.append(float(p))
    return series, fallbacks, zero_filled


def compute_power_curve(powers, durations=DURATIONS_S):
    """Best average power over any window of d consecutive samples, per d."""
    n = len(powers)
    cs = list(accumulate(powers, initial=0.0))
    curve = []
    for d in durations:
        if d > n:
            break
        # pairs (cs[i + d], cs[i]) for every window start i
        best = max(map(sub, cs[d:], cs)) / d
        curve.append((d, best))
    return curve


def compute_histogram(powers):
    """Seconds spent at each whole-watt power value (floored), sorted by watts."""
    counts = Counter(max(0, int(p)) for p in powers)
    return sorted(counts.items())


def insert_batches(conn, table, columns, rows):
    cursor = conn.cursor()
    one_row = "(" + ", ".join(["%s"] * len(columns)) + ")"
    for start in range(0, len(rows), BATCH_SIZE):
        chunk = rows[start:start + BATCH_SIZE]
        sql = (
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES "
            + ", ".join([one_row] * len(chunk))
        )
        cursor.execute(sql, tuple(v for row in chunk for v in row))


def verify(conn, path, powers, curve, hist):
    n = len(powers)
    cur = conn.cursor()

    cur.execute(
        "SELECT COUNT(*), MAX(duration_s) FROM activity_power_curve WHERE path = %s", (path,)
    )
    curve_rows, longest = cur.fetchone()
    cur.execute(
        "SELECT COUNT(*), SUM(seconds), MAX(power_w) FROM activity_power_histogram WHERE path = %s",
        (path,),
    )
    hist_rows, hist_seconds, top_watts = cur.fetchone()
    cur.execute("SELECT avg_power FROM activity_summary WHERE path = %s", (path,))
    row = cur.fetchone()
    summary_avg = row[0] if row else None

    by_duration = dict(curve)

    print(f"\nPower curve: {curve_rows} rows in the database (longest duration {longest} s)")
    print(f"  1 s best {fmt(by_duration.get(1))} W vs highest raw sample {fmt(max(powers))} W")
    for d in (5, 60, 300, 1200, 3600):
        if d in by_duration:
            print(f"  best {d:>4} s: {fmt(by_duration[d])} W")

    avg = sum(powers) / n
    print(f"  average power of the ride: {fmt(avg)} W | summary {fmt(summary_avg)} W")

    print(f"\nPower histogram: {hist_rows} rows in the database (highest bucket {top_watts} W)")
    print(f"  seconds stored {hist_seconds} vs samples in ride {n}:",
          "match" if hist_seconds == n else "MISMATCH")
    zero_band = sum(s for w, s in hist if w < 10)
    print(f"  time in the 0-9 W band: {zero_band} s ({100 * zero_band / n:.1f}% of the ride)")
    bands = Counter()
    for w, s in hist:
        if w >= 10:
            bands[(w // 10) * 10] += s
    if bands:
        band, secs = bands.most_common(1)[0]
        print(f"  most time above 10 W: the {band}-{band + 9} W band ({secs} s)")


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    file_path = sys.argv[1]
    name = os.path.basename(file_path)
    if not (name.startswith("ride_") and name.endswith(".json")):
        sys.exit("Expected a file named ride_<path>.json (as saved by inspect_ride_stream.py)")
    path = name[len("ride_"):-len(".json")]

    with open(file_path) as f:
        data = json.load(f)
    session_data = data.get("session_data")
    if not isinstance(session_data, list) or not session_data:
        sys.exit("No session_data in that file.")

    powers, fallbacks, zero_filled = power_series(session_data)
    curve = compute_power_curve(powers)
    hist = compute_histogram(powers)
    print(f"Ride {path} ({data.get('name')!r}): {len(powers)} samples, "
          f"{len(curve)} curve points, {len(hist)} histogram buckets")
    if fallbacks or zero_filled:
        print(f"  Note: {fallbacks} samples used smoothed power, {zero_filled} had no power (counted as 0 W)")

    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM activities WHERE path = %s", (path,))
        if cur.fetchone() is None:
            sys.exit(f"{path} is not in the activities table, run the activity sync first.")

        cur.execute("DELETE FROM activity_power_curve WHERE path = %s", (path,))
        cur.execute("DELETE FROM activity_power_histogram WHERE path = %s", (path,))
        insert_batches(conn, "activity_power_curve", ["path", "duration_s", "best_power_w"],
                       [(path, d, b) for d, b in curve])
        insert_batches(conn, "activity_power_histogram", ["path", "power_w", "seconds"],
                       [(path, w, s) for w, s in hist])
        conn.commit()
        print("Loaded.")
        verify(conn, path, powers, curve, hist)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
