"""
A fake partner API for the lab.
Run with:  flask --app mock_partner_api run
Behaviour: API key auth, cursor pagination, a total count, random rate limiting
(429 with Retry-After), and deliberately dirty data (duplicates, bad currency,
negative or missing amounts, mixed date formats).
"""
import random
from datetime import datetime, timedelta

from flask import Flask, jsonify, request

app = Flask(__name__)

API_KEYS = {"key-a": "partner_a", "key-b": "partner_b"}
VALID_CURRENCIES = {"NGN", "USD", "GBP"}
SUCCESS_STATUSES = {"success", "successful", "completed"}
CALLS = {"count": 0}


def build_day(partner_id: str, day: str) -> list[dict]:
    """Same partner and day always give the same data."""
    rng = random.Random(f"{partner_id}-{day}")
    start = datetime.fromisoformat(day)
    prefix = partner_id[-1].upper()
    rows = []

    for i in range(1, 501):
        ts = start + timedelta(seconds=rng.randint(0, 86399))
        row = {
            "transaction_id": f"{prefix}{day.replace('-', '')}{i:05d}",
            "merchant_id": f"M{rng.randint(1, 40):03d}",
            "amount": round(rng.uniform(500, 250000), 2),
            "currency": rng.choice(["NGN"] * 8 + ["USD", "GBP"]),
            "status": rng.choice(
                ["success", "successful", "completed", "failed", "declined", "pending"]
            ),
            "transaction_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
        }
        roll = rng.random()
        if roll < 0.02:
            row["amount"] = -row["amount"]                     # negative amount
        elif roll < 0.04:
            row["currency"] = "XXX"                            # invalid currency
        elif roll < 0.05:
            row["amount"] = None                               # missing amount
        elif roll < 0.07:
            row["transaction_ts"] = ts.strftime("%d/%m/%Y %H:%M:%S")  # other format
        rows.append(row)

    rows.extend(dict(r) for r in rng.sample(rows, 10))         # duplicates
    rng.shuffle(rows)
    return rows


def authenticate():
    key = request.headers.get("X-API-Key")
    return API_KEYS.get(key)


def maybe_rate_limit():
    CALLS["count"] += 1
    if CALLS["count"] % 7 == 0:
        resp = jsonify({"error": "rate limit exceeded"})
        resp.status_code = 429
        resp.headers["Retry-After"] = "1"
        return resp
    return None


@app.get("/transactions")
def transactions():
    partner_id = authenticate()
    if not partner_id:
        return jsonify({"error": "unauthorised"}), 401
    limited = maybe_rate_limit()
    if limited:
        return limited

    day = request.args.get("date")
    limit = min(int(request.args.get("limit", 100)), 100)
    offset = int(request.args.get("cursor") or 0)

    rows = build_day(partner_id, day)
    page = rows[offset: offset + limit]
    next_offset = offset + limit
    return jsonify(
        {
            "data": page,
            "total": len(rows),
            "next_cursor": str(next_offset) if next_offset < len(rows) else None,
        }
    )


@app.get("/daily-report")
def daily_report():
    partner_id = authenticate()
    if not partner_id:
        return jsonify({"error": "unauthorised"}), 401
    limited = maybe_rate_limit()
    if limited:
        return limited

    day = request.args.get("date")
    seen, total = set(), 0.0
    for r in build_day(partner_id, day):
        if r["transaction_id"] in seen:
            continue
        seen.add(r["transaction_id"])
        if (
            r["status"] in SUCCESS_STATUSES
            and r["amount"] is not None
            and r["amount"] > 0
            and r["currency"] in VALID_CURRENCIES
        ):
            total += r["amount"]
    return jsonify({"date": day, "total_amount": round(total, 2)})
