"""
Load the stream of ONE ride into Azure SQL (table: activity_stream), thinned
to 5 second buckets.

Reads the JSON file that inspect_ride_stream.py saved (ride_<path>.json), so
nothing is fetched from Xert again. Running it twice replaces that ride's rows
instead of duplicating them.

Usage:
    export AZURE_SQL_SERVER="chaac-xert.database.windows.net"
    export AZURE_SQL_USER="chaac"
    export AZURE_SQL_PASSWORD="your_password"
    export AZURE_SQL_DATABASE="xertdb"
    python3 load_one_ride.py ride_<path>.json
"""

import json
import os
import sys
import time

import pymssql

BATCH_SIZE = 250  # rows per INSERT statement

BUCKET_SECONDS = 5

# power_w     = mean raw power in the bucket        power_max_w = highest raw power in the bucket
# power_smooth_max_w = highest value of Xert's `power` (trailing 5 s mean of raw power) in the bucket;
#                      this is the figure Xert compares with MPA, so it keeps breakthroughs visible
# mpa         = LOWEST MPA in the bucket (so a dip toward 0 is never smoothed away)
# hr, cadence, speed_kmh, altitude_m = mean of the non-empty samples
# distance_m, tws, lws, hws, pws, xds = value at the end of the bucket (running totals)
# proximity   = highest value in the bucket (Xert field, above 1 seems to mean beyond MPA, to be verified)
COLUMNS = [
    "path", "offset_seconds", "power_w", "power_max_w", "power_smooth_max_w", "hr", "cadence",
    "speed_kmh", "altitude_m", "distance_m", "mpa", "target_power_w",
    "tws", "lws", "hws", "pws", "xds", "proximity",
]


def num(value):
    return None if value is None else float(value)


def _mean(values):
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


def _last(values):
    for v in reversed(values):
        if v is not None:
            return v
    return None


def build_rows(path, session_data, size=BUCKET_SECONDS):
    """Thin the 1 s rows to `size` second buckets, as tuples in COLUMNS order.

    Deliberately left out: lat/lng (ride start/end points reveal where you
    live), colour strings and other display-only or sparse fields. The full
    response stays in the local JSON file, so columns can be added later.
    """
    buckets = {}
    for i, r in enumerate(session_data):
        offset = r.get("t_scaled")
        if offset is None:
            raise ValueError(f"Row {i} has no t_scaled value, cannot place it in time")
        buckets.setdefault(int(offset) // size * size, []).append(r)

    rows = []
    for start in sorted(buckets):
        rs = buckets[start]

        def col(key, scale=None):
            vals = [num(r.get(key)) for r in rs]
            if scale is not None:
                vals = [None if v is None else v / scale for v in vals]
            return vals

        raw = [v for v in col("p_raw") if v is not None]
        if not raw:  # fall back to the smoothed value if raw is missing
            raw = [v for v in col("power") if v is not None]
        smooth = [v for v in col("power") if v is not None]
        mpas = [v for v in col("mpa") if v is not None]
        rows.append((
            path,
            start,
            sum(raw) / len(raw) if raw else None,
            max(raw) if raw else None,
            max(smooth) if smooth else None,
            _mean(col("hr")),
            _mean(col("cad")),
            _mean(col("spd", 1000.0)),  # spd is metres per hour -> km/h
            _mean(col("alt")),
            _last(col("dist")),
            min(mpas) if mpas else None,
            _mean(col("tgt")),
            _last(col("tws")),
            _last(col("lws")),
            _last(col("hws")),
            _last(col("pws")),
            _last(col("xds")),
            max((v for v in col("proximity") if v is not None), default=None),
        ))
    return rows


def insert_rows(conn, rows):
    cursor = conn.cursor()
    one_row = "(" + ", ".join(["%s"] * len(COLUMNS)) + ")"
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start:start + BATCH_SIZE]
        sql = (
            f"INSERT INTO activity_stream ({', '.join(COLUMNS)}) VALUES "
            + ", ".join([one_row] * len(batch))
        )
        cursor.execute(sql, tuple(v for row in batch for v in row))


def connect(attempts=4, delay=20):
    # Retries cover the serverless cold start (error 40613).
    server = os.environ.get("AZURE_SQL_SERVER")
    user = os.environ.get("AZURE_SQL_USER")
    password = os.environ.get("AZURE_SQL_PASSWORD")
    database = os.environ.get("AZURE_SQL_DATABASE")
    missing = [n for n, v in [("AZURE_SQL_SERVER", server), ("AZURE_SQL_USER", user),
                              ("AZURE_SQL_PASSWORD", password), ("AZURE_SQL_DATABASE", database)] if not v]
    if missing:
        sys.exit(f"Missing environment variables: {', '.join(missing)}")

    for attempt in range(1, attempts + 1):
        try:
            return pymssql.connect(server=server, user=user, password=password, database=database)
        except Exception as e:
            print(f"Connection attempt {attempt}/{attempts} failed: {e}")
            if attempt == attempts:
                raise
            print(f"Retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2


def fmt(value, digits=2):
    return "n/a" if value is None else f"{value:.{digits}f}"


def verify(conn, path):
    cur = conn.cursor()
    cur.execute(
        """SELECT COUNT(*), MIN(offset_seconds), MAX(offset_seconds), AVG(power_w),
                  MAX(power_max_w), MAX(speed_kmh), MAX(distance_m),
                  MIN(altitude_m), MAX(altitude_m), MIN(mpa), MAX(mpa)
           FROM activity_stream WHERE path = %s""",
        (path,),
    )
    n, lo, hi, avg_p, max_p, max_spd, max_dist, alt_lo, alt_hi, mpa_lo, mpa_hi = cur.fetchone()
    cur.execute("SELECT COUNT(*) FROM activity_stream WHERE path = %s AND proximity > 1", (path,))
    over_buckets = cur.fetchone()[0]
    cur.execute(
        "SELECT TOP 1 tws, lws, hws, pws FROM activity_stream WHERE path = %s ORDER BY offset_seconds DESC",
        (path,),
    )
    last = cur.fetchone()
    cur.execute(
        "SELECT xss, xlss, xhss, xpss, avg_power, max_power, distance_km FROM activity_summary WHERE path = %s",
        (path,),
    )
    summ = cur.fetchone()

    print(f"\nRows in activity_stream for this ride: {n} (offsets {lo}..{hi}, {BUCKET_SECONDS} s buckets)")
    print(f"Altitude {fmt(alt_lo, 1)} .. {fmt(alt_hi, 1)} m | max speed {fmt(max_spd)} km/h")
    print(f"MPA in the stream: lowest {fmt(mpa_lo, 1)} W, highest {fmt(mpa_hi, 1)} W")
    print(f"Buckets where proximity is above 1 (power beyond MPA): {over_buckets}")
    if summ:
        xss, xlss, xhss, xpss, s_avg_power, s_max_power, s_km = summ
        print("\nCross-check against activity_summary:")
        print(f"  distance   stream last row {fmt(max_dist / 1000 if max_dist else None)} km  | summary {fmt(s_km)} km")
        print(f"  avg power  stream mean of buckets {fmt(avg_p)}  | summary {fmt(s_avg_power)}  (close, not exact: thinned)")
        print(f"  max power  stream power_max_w {fmt(max_p)}  | summary {fmt(s_max_power)}")
        if last:
            print(f"  XSS total  stream tws {fmt(last[0])}  | summary xss {fmt(xss)}")
            print(f"  XSS low    stream lws {fmt(last[1])}  | summary xlss {fmt(xlss)}")
            print(f"  XSS high   stream hws {fmt(last[2])}  | summary xhss {fmt(xhss)}")
            print(f"  XSS peak   stream pws {fmt(last[3])}  | summary xpss {fmt(xpss)}")
    else:
        print("No activity_summary row found for this path, skipping the cross-check.")

    try:
        cur.execute(
            """SELECT breakthrough, medal, sig_ftp, prev_sig_ftp, sig_pp, prev_sig_pp,
                      sig_atc, prev_sig_atc FROM activity_summary WHERE path = %s""",
            (path,),
        )
        row = cur.fetchone()
        if row:
            print("\nSignature in the summary (this ride vs before):")
            print(f"  breakthrough={row[0]} medal={row[1]}")
            print(f"  FTP {fmt(row[2], 1)} (before {fmt(row[3], 1)}) | PP {fmt(row[4], 1)} (before {fmt(row[5], 1)}) "
                  f"| HIE {fmt(row[6], 0)} (before {fmt(row[7], 0)})")
    except Exception as e:  # a column name may differ; the cross-check above is the important part
        print(f"\n(Could not read the signature columns: {e})")


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

    rows = build_rows(path, session_data)
    print(f"Ride {path} ({data.get('name')!r}): {len(rows)} rows to load")

    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM activities WHERE path = %s", (path,))
        if cur.fetchone() is None:
            sys.exit(f"{path} is not in the activities table, run the activity sync first.")

        cur.execute("DELETE FROM activity_stream WHERE path = %s", (path,))
        insert_rows(conn, rows)
        conn.commit()
        print("Loaded.")
        verify(conn, path)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
