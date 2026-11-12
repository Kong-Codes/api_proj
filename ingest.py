"""
Pulls one day of transactions from each partner API and loads the raw layer.

Gate 1 (at the API):
  - retries with backoff on 429 and 5xx (honours Retry-After)
  - cursor pagination with a loop guard
  - schema check: required fields must be present, unexpected fields are logged
  - count check: rows fetched must equal the total the API reported

Loads are idempotent: each partner/day is a batch, and re-running it replaces
that batch instead of adding duplicates.

Usage:
    python ingest.py                      # yesterday (UTC)
    python ingest.py --date 2026-11-05
"""
import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import snowflake.connector
from requests.adapters import HTTPAdapter
from snowflake.connector.pandas_tools import write_pandas
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("ingest")

REQUIRED_FIELDS = {
    "transaction_id",
    "merchant_id",
    "amount",
    "currency",
    "status",
    "transaction_ts",
}
PAGE_SIZE = 100
TIMEOUT_SECONDS = 30


class SchemaError(Exception):
    """The API response is missing fields we depend on."""


class CountError(Exception):
    """Rows fetched do not match the total the API reported."""


# ---------- configuration ----------

def load_partners() -> list[dict]:
    partners = []
    for partner_id in os.getenv("PARTNER_IDS").split(","):
        partner_id = partner_id.strip()
        prefix = partner_id.upper()
        partners.append(
            {
                "id": partner_id,
                "base_url": os.getenv(f"{prefix}_URL").rstrip("/"),
                "api_key": os.getenv(f"{prefix}_KEY"),
            }
        )
    return partners


def snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="RAW",
        autocommit=False,
    )


# ---------- API layer ----------

def build_session(api_key: str) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,                       # 1s, 2s, 4s, 8s, 16s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"X-API-Key": api_key})
    return session


def fetch_transactions(session: requests.Session, partner: dict, day: str):
    """Follow the cursor until the API says there are no more pages."""
    rows, cursor, seen_cursors, expected_total = [], None, set(), None

    while True:
        params = {"date": day, "limit": PAGE_SIZE}
        if cursor:
            params["cursor"] = cursor

        resp = session.get(
            f"{partner['base_url']}/transactions",
            params=params,
            timeout=TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        body = resp.json()

        rows.extend(body["data"])
        expected_total = body.get("total", expected_total)
        cursor = body.get("next_cursor")

        if not cursor:
            break
        if cursor in seen_cursors:              # guard against an endless loop
            raise RuntimeError(f"Cursor {cursor} repeated, stopping")
        seen_cursors.add(cursor)

    return rows, expected_total


def fetch_daily_report(session: requests.Session, partner: dict, day: str) -> float:
    resp = session.get(
        f"{partner['base_url']}/daily-report",
        params={"date": day},
        timeout=TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    return resp.json()["total_amount"]


# ---------- checks ----------

def check_schema(rows: list[dict]) -> None:
    seen_fields = set()
    for row in rows:
        seen_fields.update(row.keys())

    missing = REQUIRED_FIELDS - seen_fields
    if missing:
        raise SchemaError(f"Missing required fields: {sorted(missing)}")

    unexpected = seen_fields - REQUIRED_FIELDS
    if unexpected:
        log.warning("Unexpected fields from API (ignored): %s", sorted(unexpected))


def check_count(rows: list[dict], expected_total) -> None:
    if expected_total is not None and len(rows) != expected_total:
        raise CountError(f"Fetched {len(rows)} rows but the API reported {expected_total}")


# ---------- load layer ----------

def to_dataframe(rows: list[dict], partner_id: str, batch_id: str) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=sorted(REQUIRED_FIELDS))
    # Raw layer keeps values as received: text, nulls stay null
    df = df.astype(object).where(df.notna(), None)
    for col in df.columns:
        df[col] = df[col].map(lambda v: None if v is None else str(v))
    df["partner_id"] = partner_id
    df["_batch_id"] = batch_id
    df.columns = [c.upper() for c in df.columns]
    return df


def load_batch(conn, df: pd.DataFrame, batch_id: str) -> None:
    """Replace the batch in one transaction, so a re-run never duplicates."""
    cur = conn.cursor()
    try:
        cur.execute("delete from PARTNER_TRANSACTIONS where _BATCH_ID = %s", (batch_id,))
        write_pandas(
            conn, df, "PARTNER_TRANSACTIONS",
            schema="RAW", quote_identifiers=False, auto_create_table=False,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def load_daily_report(conn, partner_id: str, day: str, total: float) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            "delete from PARTNER_DAILY_REPORTS where PARTNER_ID = %s and REPORT_DATE = %s",
            (partner_id, day),
        )
        cur.execute(
            "insert into PARTNER_DAILY_REPORTS (PARTNER_ID, REPORT_DATE, TOTAL_AMOUNT) "
            "values (%s, %s, %s)",
            (partner_id, day, total),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def write_log(conn, batch_id, partner_id, day, fetched, expected, status, error=None):
    cur = conn.cursor()
    try:
        cur.execute(
            "insert into INGESTION_LOG "
            "(BATCH_ID, PARTNER_ID, LOAD_DATE, ROWS_FETCHED, ROWS_EXPECTED, STATUS, ERROR_MESSAGE) "
            "values (%s, %s, %s, %s, %s, %s, %s)",
            (batch_id, partner_id, day, fetched, expected, status, error),
        )
        conn.commit()
    finally:
        cur.close()


# ---------- orchestration ----------

def run_partner(conn, partner: dict, day: str) -> None:
    batch_id = f"{partner['id']}_{day}"
    fetched, expected = 0, None
    try:
        session = build_session(partner["api_key"])

        rows, expected = fetch_transactions(session, partner, day)
        fetched = len(rows)
        log.info("%s: fetched %s rows (API total %s)", partner["id"], fetched, expected)

        check_schema(rows)
        check_count(rows, expected)

        df = to_dataframe(rows, partner["id"], batch_id)
        load_batch(conn, df, batch_id)

        report_total = fetch_daily_report(session, partner, day)
        load_daily_report(conn, partner["id"], day, report_total)

        write_log(conn, batch_id, partner["id"], day, fetched, expected, "success")
        log.info("%s: batch %s loaded", partner["id"], batch_id)

    except Exception as exc:
        log.error("%s: batch %s failed: %s", partner["id"], batch_id, exc)
        write_log(conn, batch_id, partner["id"], day, fetched, expected, "failed", str(exc)[:500])
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=(datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Day to load, YYYY-MM-DD (default: yesterday, UTC)",
    )
    args = parser.parse_args()

    conn = snowflake_connection()
    failures = 0
    try:
        for partner in load_partners():
            try:
                run_partner(conn, partner, args.date)
            except Exception:
                failures += 1          # one bad partner must not block the others
    finally:
        conn.close()

    if failures:
        log.error("%s partner(s) failed", failures)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
