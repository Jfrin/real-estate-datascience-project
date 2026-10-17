"""Data-quality checks for the NYC real-estate Cassandra tables.

Each check raises an exception on failure, which turns the Airflow task red.
Primary keys (permit_si_no, document_id, bbl) can never be null in Cassandra,
so the null check looks at the other columns the dashboards/reports rely on.
"""
import os

from airflow.decorators import task
from airflow.models import Variable
from cassandra.cluster import Cluster

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "nyc_realestate")

# Fail if row count falls by more than this fraction vs. the last good run.
MAX_ROW_DROP = float(os.environ.get("DQ_MAX_ROW_DROP", "0.20"))

# table -> {column: max allowed null fraction}
KEY_COLUMNS = {
    "permits_current": {"borough": 0.02, "filing_date": 0.05, "latitude": 0.20, "longitude": 0.20},
    "sales_acris": {"borough": 0.02, "doc_type": 0.02, "document_date": 0.05},
    "parcels_pluto": {"borough": 0.02, "bldg_class": 0.10, "lot_area": 0.10},
}
TABLES = list(KEY_COLUMNS)


def profile_table(table):
    """One scan of the table: returns (row_count, {column: null_count})."""
    cols = list(KEY_COLUMNS[table])
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    try:
        session = cluster.connect(KEYSPACE)
        session.default_fetch_size = 5000
        rows = 0
        nulls = {c: 0 for c in cols}
        for row in session.execute(f"SELECT {', '.join(cols)} FROM {table}"):
            rows += 1
            for c in cols:
                if getattr(row, c) is None:
                    nulls[c] += 1
        return rows, nulls
    finally:
        cluster.shutdown()


@task
def check_not_empty(table):
    rows, _ = profile_table(table)
    if rows == 0:
        raise ValueError(f"{table} is empty")
    print(f"{table}: {rows} rows")


@task
def check_row_count_drop(table):
    rows, _ = profile_table(table)
    var = f"dq_last_count_{table}"
    prev = Variable.get(var, default_var=None)
    if prev is not None:
        prev = int(prev)
        if prev > 0 and rows < prev * (1 - MAX_ROW_DROP):
            # Don't overwrite the baseline, so the next run still compares to the good count.
            raise ValueError(
                f"{table} row count dropped from {prev} to {rows} "
                f"({(prev - rows) / prev:.0%}, limit {MAX_ROW_DROP:.0%})"
            )
    Variable.set(var, rows)
    print(f"{table}: {rows} rows (previous {prev})")


@task
def check_key_columns_not_null(table):
    rows, nulls = profile_table(table)
    if rows == 0:
        raise ValueError(f"{table} is empty")
    bad = []
    for col, limit in KEY_COLUMNS[table].items():
        frac = nulls[col] / rows
        print(f"{table}.{col}: {frac:.1%} null (limit {limit:.0%})")
        if frac > limit:
            bad.append(f"{col} {frac:.1%} null (limit {limit:.0%})")
    if bad:
        raise ValueError(f"{table} null check failed: " + "; ".join(bad))


def table_checks(table):
    """Wire the three checks for one table in order; call inside a DAG."""
    empty = check_not_empty.override(task_id=f"{table}_not_empty")(table)
    drop = check_row_count_drop.override(task_id=f"{table}_row_count_drop")(table)
    nulls = check_key_columns_not_null.override(task_id=f"{table}_key_columns_not_null")(table)
    empty >> drop >> nulls
    return empty, nulls  # first and last task, for chaining
