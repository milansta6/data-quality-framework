"""Synthetic sample data with known, deliberately injected quality problems.

Everything here is fake and generated from a fixed random seed. Dates are relative
to "now", so freshness rules behave the same on every run.

Tables
------
* ``customers``         - customer master data
* ``accounts``          - accounts owned by customers
* ``transactions_raw``  - raw transactions as landed (Bronze)
* ``transactions``      - cleaned transactions (Silver), 3 rows lost on the way

Injected problems (so the demo report has something to show)
------------------------------------------------------------
* customers: missing and malformed e-mails, duplicate ids, unknown country,
  birth date in the future
* accounts: unknown customer ids (orphans), invalid currency, negative savings
  balance, closed account with money on it, stale ``updated_at``
* transactions: amounts out of range, unknown account ids, booking before the
  account was opened, rows lost between Bronze and Silver
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

FIRST_NAMES = ["Alex", "Sam", "Robin", "Kim", "Jordan", "Taylor", "Chris", "Morgan", "Jamie", "Max"]
LAST_NAMES = ["Smith", "Novak", "Meyer", "Rossi", "Silva", "Jansen", "Horvat", "Berg", "Klein"]
COUNTRIES = ["DE", "AT", "CH", "FR", "NL", "IT", "ES", "RS"]
SEGMENTS = ["RETAIL", "PREMIUM", "BUSINESS"]
ACCOUNT_TYPES = ["CHECKING", "SAVINGS", "CREDIT_CARD"]
CURRENCIES = ["EUR", "USD", "CHF", "GBP"]
CHANNELS = ["CARD", "ONLINE", "BRANCH", "ATM", "TRANSFER"]


def _money(rng: random.Random, low: float, high: float) -> Decimal:
    return Decimal(str(round(rng.uniform(low, high), 2)))


def generate_demo_rows(
    n_customers: int = 200,
    n_accounts: int = 300,
    n_transactions: int = 1000,
    seed: int = 42,
    now: datetime | None = None,
) -> dict[str, list[tuple]]:
    """Return plain Python rows for every demo table (no Spark needed)."""
    rng = random.Random(seed)
    now = (now or datetime.now()).replace(microsecond=0)
    today = now.date()

    # ---------------------------------------------------------------- customers
    customers = []
    for i in range(1, n_customers + 1):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        customers.append(
            [
                f"C{i:05d}",
                first,
                last,
                f"{first.lower()}.{last.lower()}{i}@example.com",
                rng.choice(COUNTRIES),
                rng.choice(SEGMENTS),
                date(1950, 1, 1) + timedelta(days=rng.randint(0, 365 * 55)),
                now - timedelta(days=rng.randint(30, 2000)),
            ]
        )
    for idx in (5, 17, 42):
        customers[idx][3] = None  # missing e-mail
    for idx, bad in zip(
        (8, 23, 61, 99), ("no-at-sign.example.com", "x@", "@example.com", "a b@c.de")
    ):
        customers[idx][3] = bad  # malformed e-mail
    customers[30][4] = "XX"  # unknown country
    customers[77][4] = "XX"
    customers[120][6] = today + timedelta(days=400)  # born in the future
    customers.append(list(customers[10]))  # duplicate customer rows
    customers.append(list(customers[11]))

    customer_ids = [c[0] for c in customers[:n_customers]]

    # ---------------------------------------------------------------- accounts
    accounts = []
    for i in range(1, n_accounts + 1):
        acc_type = rng.choice(ACCOUNT_TYPES)
        balance = _money(rng, -2000, 0) if acc_type == "CREDIT_CARD" else _money(rng, 0, 50000)
        accounts.append(
            [
                f"A{i:06d}",
                rng.choice(customer_ids),
                acc_type,
                rng.choice(CURRENCIES),
                balance,
                today - timedelta(days=rng.randint(60, 3000)),
                rng.choices(["ACTIVE", "CLOSED", "BLOCKED"], weights=[85, 10, 5])[0],
                now - timedelta(hours=30),  # last update 30h ago -> stale for a 24h SLA
            ]
        )
    for idx in (3, 150, 260):
        accounts[idx][1] = "C99999"  # customer does not exist
    accounts[12][3] = "EURO"  # invalid currency
    accounts[200][3] = "eur"
    closed = [a for a in accounts if a[6] == "CLOSED"]
    for acc in closed:
        acc[4] = Decimal("0.00")
    closed[0][4] = Decimal("125.50")  # closed account still holds money
    savings = [a for a in accounts if a[2] == "SAVINGS"]
    savings[0][4] = Decimal("-15.00")  # negative savings balance
    savings[1][4] = Decimal("-0.01")

    account_ids = [a[0] for a in accounts]
    opened = {a[0]: a[5] for a in accounts}

    # ---------------------------------------------------------------- transactions
    raw = []
    for i in range(1, n_transactions + 1):
        acc = rng.choice(account_ids)
        ts = now - timedelta(minutes=rng.randint(30, 60 * 24 * 7))
        raw.append(
            [
                f"T{i:08d}",
                acc,
                _money(rng, -2500, 2500),
                rng.choice(CURRENCIES[:3]),
                ts,
                rng.choice(CHANNELS),
                now - timedelta(hours=2),
            ]
        )
    for idx in (14, 400, 777):
        raw[idx][2] = Decimal("2500000.00")  # suspiciously large amount
    raw[55][1] = "A999999"  # unknown account
    raw[56][1] = "A999998"
    # booked before the account was opened
    raw[90][4] = datetime.combine(opened[raw[90][1]], datetime.min.time()) - timedelta(days=3)

    lost = {100, 101, 102}  # rows lost between Bronze and Silver
    silver = [list(r) for i, r in enumerate(raw) if i not in lost]

    return {
        "customers": [tuple(r) for r in customers],
        "accounts": [tuple(r) for r in accounts],
        "transactions_raw": [tuple(r) for r in raw],
        "transactions": [tuple(r) for r in silver],
    }


def demo_schemas():
    from pyspark.sql import types as T

    money = T.DecimalType(18, 2)
    transactions = T.StructType(
        [
            T.StructField("transaction_id", T.StringType()),
            T.StructField("account_id", T.StringType()),
            T.StructField("amount", money),
            T.StructField("currency", T.StringType()),
            T.StructField("transaction_ts", T.TimestampType()),
            T.StructField("channel", T.StringType()),
            T.StructField("ingested_at", T.TimestampType()),
        ]
    )
    return {
        "customers": T.StructType(
            [
                T.StructField("customer_id", T.StringType()),
                T.StructField("first_name", T.StringType()),
                T.StructField("last_name", T.StringType()),
                T.StructField("email", T.StringType()),
                T.StructField("country", T.StringType()),
                T.StructField("segment", T.StringType()),
                T.StructField("birth_date", T.DateType()),
                T.StructField("created_at", T.TimestampType()),
            ]
        ),
        "accounts": T.StructType(
            [
                T.StructField("account_id", T.StringType()),
                T.StructField("customer_id", T.StringType()),
                T.StructField("account_type", T.StringType()),
                T.StructField("currency", T.StringType()),
                T.StructField("balance", money),
                T.StructField("opened_date", T.DateType()),
                T.StructField("status", T.StringType()),
                T.StructField("updated_at", T.TimestampType()),
            ]
        ),
        "transactions_raw": transactions,
        "transactions": transactions,
    }


def create_demo_tables(spark: SparkSession, seed: int = 42, **kwargs) -> dict[str, DataFrame]:
    """Create the demo tables as Spark DataFrames."""
    rows = generate_demo_rows(seed=seed, **kwargs)
    schemas = demo_schemas()
    return {name: spark.createDataFrame(data, schemas[name]) for name, data in rows.items()}
