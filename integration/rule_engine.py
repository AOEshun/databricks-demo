"""
Rule engine for Silver DLT quarantine routing.

Pure-Python module — no Spark import.  Consumed by the DLT pipeline notebook
to build the clean_predicate and the failed_rules ARRAY<STRING> column.

Usage example
-------------
from rule_engine import build_clean_predicate, build_failed_rules_expr

DROP_RULES = {
    "order_ts_not_null":      "order_ts IS NOT NULL",
    "customer_id_not_null":   "customer_id IS NOT NULL",
    "order_currency_not_null":"order_currency IS NOT NULL",
    "order_total_positive":   "order_total >= 0",
    "order_amount_positive":  "order_amount >= 0",
}

clean_predicate      = build_clean_predicate(DROP_RULES)
failed_rules_expr    = build_failed_rules_expr(DROP_RULES)

# Cleansed stream  — rows that pass every drop rule
df.filter(clean_predicate)

# Quarantine stream — rows that fail at least one drop rule
df.filter(f"NOT ({clean_predicate})").withColumn("failed_rules", failed_rules_expr)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # pyspark.sql.Column is only used as a type annotation; never imported at
    # runtime so the module stays Spark-free for unit-testing.
    from pyspark.sql import Column


def build_clean_predicate(drop_rules: dict[str, str]) -> str:
    """Return a SQL predicate string that is True when ALL drop rules pass.

    Parameters
    ----------
    drop_rules:
        Mapping of {rule_name: sql_predicate}.  A row must satisfy every
        predicate to be considered clean.

    Returns
    -------
    str
        A SQL expression string like ``(rule_a) AND (rule_b) AND ...``.
        Returns ``"1=1"`` (always-true) when drop_rules is empty so that
        callers can safely apply it as a filter without special-casing.
    """
    if not drop_rules:
        return "1=1"
    return " AND ".join(f"({predicate})" for predicate in drop_rules.values())


def build_failed_rules_expr(drop_rules: dict[str, str]) -> "Column":
    """Return a Spark Column expression that produces ARRAY<STRING> of failed rule names.

    Each element of the array is the *name* (key) of a drop rule that the row
    violates.  A row with no violations produces an empty array ``[]``.

    Parameters
    ----------
    drop_rules:
        Mapping of {rule_name: sql_predicate} — same dict passed to
        :func:`build_clean_predicate`.

    Returns
    -------
    pyspark.sql.Column
        A Column expression suitable for ``.withColumn("failed_rules", expr)``.

    Notes
    -----
    Spark is imported locally so that this module can be imported in pure-Python
    test environments without a SparkSession.
    """
    # Local import keeps the module Spark-free at the module level.
    from pyspark.sql import functions as F  # noqa: PLC0415

    # Build an array of (rule_name or NULL) for each rule, then filter NULLs.
    # CASE WHEN NOT (predicate) THEN 'rule_name' END → name when violated, else NULL
    rule_columns = [
        F.when(~F.expr(predicate), F.lit(name))
        for name, predicate in drop_rules.items()
    ]

    # array_compact removes NULLs — requires Spark 3.4+; for older clusters
    # filter(x -> x is not null) is equivalent.
    failed = F.array(*rule_columns)
    return F.array_compact(failed)
