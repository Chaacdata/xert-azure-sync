"""
Xert -> Azure SQL sync script (activities + activity_summary only)
--------------------------------------------------------------------
Pulls activity data from the Xert Online API and upserts it into the
Azure SQL tables we created manually (activities, activity_summary).

This version intentionally leaves out streaming/session data and
training_status -- those come in a later step once this is working.

Set these environment variables before running:
    XERT_USERNAME       - your Xert login email
    XERT_PASSWORD       - your Xert password
    AZURE_SQL_SERVER     - e.g. chaac-xert.database.windows.net
    AZURE_SQL_USER       - e.g. chaac
    AZURE_SQL_PASSWORD   - your Azure SQL login password
    AZURE_SQL_DATABASE   - e.g. xertdb

Run:
    python3 xert_sync_azure.py
"""

import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone

import pymssql
import requests

# Retry settings for transient failures (e.g. serverless DB cold-start,
# brief network blips, Xert API hiccups).
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 15  # doubles each attempt: 15s, 30s, 60s, 120s


def with_retries(func, *args, description="operation", **kwargs):
    """Call func(*args, **kwargs), retrying on failure with exponential backoff."""
    delay = RETRY_BACKOFF_SECONDS
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_error = e
            print(f"  Attempt {attempt}/{MAX_RETRIES} failed for {description}: {e}")
            if attempt < MAX_RETRIES:
                print(f"  Retrying in {delay}s...")
                time.sleep(delay)
                delay *= 2
    raise last_error

XERT_BASE = "https://www.xertonline.com"
TOKEN_URL = f"{XERT_BASE}/oauth/token"
ACTIVITY_LIST_URL = f"{XERT_BASE}/oauth/activity"
ACTIVITY_DETAIL_URL = f"{XERT_BASE}/oauth/activity/{{path}}"
TRAINING_INFO_URL = f"{XERT_BASE}/oauth/training_info"

XERT_USERNAME = os.environ.get("XERT_USERNAME")
XERT_PASSWORD = os.environ.get("XERT_PASSWORD")

AZURE_SQL_SERVER = os.environ.get("AZURE_SQL_SERVER")
AZURE_SQL_USER = os.environ.get("AZURE_SQL_USER")
AZURE_SQL_PASSWORD = os.environ.get("AZURE_SQL_PASSWORD")
AZURE_SQL_DATABASE = os.environ.get("AZURE_SQL_DATABASE")

# How far back to look on the very first run (Unix timestamp).
INITIAL_FROM_TS = 1420070400  # Jan 1, 2015


def check_env():
    required = {
        "XERT_USERNAME": XERT_USERNAME,
        "XERT_PASSWORD": XERT_PASSWORD,
        "AZURE_SQL_SERVER": AZURE_SQL_SERVER,
        "AZURE_SQL_USER": AZURE_SQL_USER,
        "AZURE_SQL_PASSWORD": AZURE_SQL_PASSWORD,
        "AZURE_SQL_DATABASE": AZURE_SQL_DATABASE,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        sys.exit(f"Missing environment variables: {', '.join(missing)}")


# --------------------------------------------------------------------------
# Database
# --------------------------------------------------------------------------

def _connect():
    return pymssql.connect(
        server=AZURE_SQL_SERVER,
        user=AZURE_SQL_USER,
        password=AZURE_SQL_PASSWORD,
        database=AZURE_SQL_DATABASE,
    )


def get_connection():
    # Retries here specifically cover the serverless cold-start case
    # (error 40613), where the first connection attempt after idle
    # time fails while the database wakes up.
    return with_retries(_connect, description="Azure SQL connection")


def ensure_sync_meta_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='sync_meta' AND xtype='U')
        CREATE TABLE sync_meta (
            [key]   NVARCHAR(100) PRIMARY KEY,
            [value] NVARCHAR(200)
        )
    """)
    conn.commit()


def get_last_synced_unix(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT [value] FROM sync_meta WHERE [key] = 'last_synced_unix'")
    row = cursor.fetchone()
    return int(row[0]) if row else INITIAL_FROM_TS


def set_last_synced_unix(conn, ts):
    cursor = conn.cursor()
    cursor.execute("""
        MERGE INTO sync_meta AS target
        USING (SELECT 'last_synced_unix' AS [key], %s AS [value]) AS source
        ON target.[key] = source.[key]
        WHEN MATCHED THEN UPDATE SET [value] = source.[value]
        WHEN NOT MATCHED THEN INSERT ([key], [value]) VALUES (source.[key], source.[value]);
    """, (str(ts),))
    conn.commit()


# --------------------------------------------------------------------------
# Xert API
# --------------------------------------------------------------------------

def _get_access_token(session):
    resp = session.post(
        TOKEN_URL,
        auth=("xert_public", "xert_public"),
        data={
            "grant_type": "password",
            "username": XERT_USERNAME,
            "password": XERT_PASSWORD,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_access_token(session):
    # Retry only covers transient issues (timeouts, brief outages) --
    # a genuine bad password will fail with 401 on every attempt and
    # still surface as a real error after retries are exhausted.
    return with_retries(_get_access_token, session, description="Xert token request")


def _api_get(session, token, url, params=None):
    resp = session.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def api_get(session, token, url, params=None):
    return with_retries(
        _api_get, session, token, url, params=params, description=f"GET {url}"
    )


# --------------------------------------------------------------------------
# Upserts
# --------------------------------------------------------------------------

def upsert_activity(conn, path, name, description, activity_type, start_date, start_unix, now_iso):
    cursor = conn.cursor()
    cursor.execute("""
        MERGE INTO activities AS target
        USING (SELECT %s AS path) AS source
        ON target.path = source.path
        WHEN MATCHED THEN UPDATE SET
            name = %s, description = %s, activity_type = %s,
            start_date = %s, start_date_unix = %s, last_synced_at = %s
        WHEN NOT MATCHED THEN INSERT
            (path, name, description, activity_type, start_date, start_date_unix, last_synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
    """, (
        path,
        name, description, activity_type, start_date, start_unix, now_iso,
        path, name, description, activity_type, start_date, start_unix, now_iso,
    ))


def upsert_activity_summary(conn, path, s, sig, session_stats, prev_sig):
    cursor = conn.cursor()
    power_source = s.get("power_source")
    activity_map_url = s.get("activity_map")
    progression = s.get("progression")
    progression_json = json.dumps(progression) if progression is not None else None

    cursor.execute("""
        MERGE INTO activity_summary AS target
        USING (SELECT %s AS path) AS source
        ON target.path = source.path
        WHEN MATCHED THEN UPDATE SET
            xss=%s, xlss=%s, xhss=%s, xpss=%s, xep=%s, focus=%s, specificity=%s,
            mep=%s, tws=%s, sp=%s, sfd=%s, difficulty=%s, difficulty_rating=%s,
            distance_km=%s, duration_sec=%s, sig_ftp=%s, sig_atc=%s, sig_pp=%s,
            medal=%s, breakthrough=%s, training_status_score=%s, freshness=%s,
            total_grams_carbs=%s, total_grams_fat=%s, max_power=%s, avg_power=%s,
            max_cadence=%s, total_elevation_gain=%s, total_calories=%s,
            prev_sig_ftp=%s, prev_sig_atc=%s, prev_sig_pp=%s, prev_sig_ltp=%s,
            power_source=%s, activity_map_url=%s, progression_json=%s
        WHEN NOT MATCHED THEN INSERT (
            path, xss, xlss, xhss, xpss, xep, focus, specificity, mep, tws, sp, sfd,
            difficulty, difficulty_rating, distance_km, duration_sec, sig_ftp, sig_atc,
            sig_pp, medal, breakthrough, training_status_score, freshness,
            total_grams_carbs, total_grams_fat, max_power, avg_power, max_cadence,
            total_elevation_gain, total_calories, prev_sig_ftp, prev_sig_atc,
            prev_sig_pp, prev_sig_ltp, power_source, activity_map_url, progression_json
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """, (
        path,
        # UPDATE values
        s.get("xss"), s.get("xlss"), s.get("xhss"), s.get("xpss"), s.get("xep"),
        s.get("focus"), s.get("specificity"), s.get("mep"), s.get("tws"), s.get("sp"),
        s.get("sfd"), s.get("difficulty"), s.get("difficulty_rating"), s.get("distance"),
        s.get("duration"), sig.get("ftp"), sig.get("atc"), sig.get("pp"), s.get("medal"),
        s.get("breakthrough"), s.get("training_status"), s.get("freshness"),
        s.get("total_grams_carbs"), s.get("total_grams_fat"),
        session_stats.get("max_power"), session_stats.get("avg_power"),
        session_stats.get("max_cadence"), session_stats.get("total_elevation_gain"),
        session_stats.get("total_calories"),
        prev_sig.get("ftp"), prev_sig.get("atc"), prev_sig.get("pp"), prev_sig.get("ltp"),
        power_source, activity_map_url, progression_json,
        # INSERT values (path + same fields again)
        path, s.get("xss"), s.get("xlss"), s.get("xhss"), s.get("xpss"), s.get("xep"),
        s.get("focus"), s.get("specificity"), s.get("mep"), s.get("tws"), s.get("sp"),
        s.get("sfd"), s.get("difficulty"), s.get("difficulty_rating"), s.get("distance"),
        s.get("duration"), sig.get("ftp"), sig.get("atc"), sig.get("pp"), s.get("medal"),
        s.get("breakthrough"), s.get("training_status"), s.get("freshness"),
        s.get("total_grams_carbs"), s.get("total_grams_fat"),
        session_stats.get("max_power"), session_stats.get("avg_power"),
        session_stats.get("max_cadence"), session_stats.get("total_elevation_gain"),
        session_stats.get("total_calories"),
        prev_sig.get("ftp"), prev_sig.get("atc"), prev_sig.get("pp"), prev_sig.get("ltp"),
        power_source, activity_map_url, progression_json,
    ))


# --------------------------------------------------------------------------
# Sync logic
# --------------------------------------------------------------------------

def sync_activities(session, token, conn):
    from_ts = get_last_synced_unix(conn)
    to_ts = int(time.time())

    data = api_get(session, token, ACTIVITY_LIST_URL, params={"from": from_ts, "to": to_ts})
    activities = data.get("activities", [])
    print(f"Found {len(activities)} activities updated since {from_ts}.")

    now_iso = datetime.now(timezone.utc).isoformat()

    for act in activities:
        path = act["path"]
        start_date = act.get("start_date", {}).get("date")
        start_unix = None
        if start_date:
            try:
                start_unix = int(
                    datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S.%f")
                    .replace(tzinfo=timezone.utc)
                    .timestamp()
                )
            except ValueError:
                pass

        upsert_activity(
            conn, path, act.get("name"), act.get("description"),
            act.get("activity_type"), start_date, start_unix, now_iso,
        )

        try:
            detail = api_get(session, token, ACTIVITY_DETAIL_URL.format(path=path))
        except requests.HTTPError as e:
            print(f"  Skipping summary for {path}: {e}")
            conn.commit()
            continue

        s = detail.get("summary", {})
        sig = s.get("sig", {})
        session_stats = s.get("session", {})
        prev_sig = s.get("prev_sig", {})
        upsert_activity_summary(conn, path, s, sig, session_stats, prev_sig)
        conn.commit()

    set_last_synced_unix(conn, to_ts)


def build_training_info_row(data, now):
    """Flatten a /oauth/training_info response into one dict per daily snapshot.

    Every nested lookup tolerates missing keys: when wotd.type is "None",
    Xert returns only {"type": "None"} with no name/workoutId/etc.
    """
    sig = data.get("signature") or {}
    tl = data.get("tl") or {}
    target = data.get("targetXSS") or {}
    wotd = data.get("wotd") or {}

    return {
        "snapshot_date": now.date().isoformat(),
        "snapshot_at": now.isoformat(),
        "status": data.get("status"),
        "source": data.get("source"),
        "weight": data.get("weight"),
        "sig_ftp": sig.get("ftp"),
        "sig_ltp": sig.get("ltp"),
        "sig_hie": sig.get("hie"),
        "sig_pp": sig.get("pp"),
        "tl_low": tl.get("low"),
        "tl_high": tl.get("high"),
        "tl_peak": tl.get("peak"),
        "tl_total": tl.get("total"),
        "target_xss_low": target.get("low"),
        "target_xss_high": target.get("high"),
        "target_xss_peak": target.get("peak"),
        "target_xss_total": target.get("total"),
        "wotd_type": wotd.get("type"),
        "wotd_name": wotd.get("name"),
        "wotd_workout_id": wotd.get("workoutId"),
        "wotd_description": wotd.get("description"),
        "wotd_difficulty": wotd.get("difficulty"),
        "wotd_url": wotd.get("url"),
    }


def upsert_daily_training_info(conn, row):
    # SQL is generated from the dict keys so the column list and the
    # parameter list can never drift out of sync.
    cols = [c for c in row if c != "snapshot_date"]
    update_set = ", ".join(f"{c} = %s" for c in cols)
    insert_cols = ", ".join(["snapshot_date"] + cols)
    insert_vals = ", ".join(["%s"] * (len(cols) + 1))

    sql = f"""
        MERGE INTO daily_training_info AS target
        USING (SELECT %s AS snapshot_date) AS source
        ON target.snapshot_date = source.snapshot_date
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
    """
    params = (
        [row["snapshot_date"]]
        + [row[c] for c in cols]
        + [row["snapshot_date"]]
        + [row[c] for c in cols]
    )
    cursor = conn.cursor()
    cursor.execute(sql, tuple(params))


def sync_training_info(session, token, conn):
    # /oauth/training_info only returns the *current* state, so history
    # exists only from the first run onward -- one row per day, rerunning
    # on the same day updates that day's row instead of adding another.
    data = api_get(session, token, TRAINING_INFO_URL)
    row = build_training_info_row(data, datetime.now(timezone.utc))
    upsert_daily_training_info(conn, row)
    conn.commit()
    print(
        f"Training info snapshot for {row['snapshot_date']}: "
        f"target XSS total={row['target_xss_total']}, wotd={row['wotd_type']}"
        + (f" ({row['wotd_name']})" if row["wotd_name"] else "")
    )


def run():
    check_env()
    conn = get_connection()
    ensure_sync_meta_table(conn)

    session = requests.Session()
    token = get_access_token(session)

    sync_activities(session, token, conn)
    sync_training_info(session, token, conn)

    conn.close()
    print("Sync complete.")


def main():
    # Top-level safety net: in an unattended/scheduled run, nobody is
    # watching the terminal. A clear message plus a non-zero exit code
    # is what lets GitHub Actions (or cron + an alerting wrapper) know
    # the run genuinely failed, rather than hanging or failing silently.
    try:
        run()
    except Exception:
        print("SYNC FAILED:", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
