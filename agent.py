"""
Day 12 — ETL Partition Pruning Verification Agent
==================================================
Pattern  : Pre-extract guardrail — verify partition filters before any bulk extract begins
Guardrail: NEVER approve an extract whose effective partition scope is unverifiable from
           available metadata, even if the filter looks syntactically correct.
"""

import json
import datetime
from typing import Any

PARTITION_METADATA = {
    "orders": {
        "partition_column": "order_date",
        "partition_type": "date",
        "earliest_partition": "2023-01-01",
        "latest_partition": "2026-07-31",
        "total_partitions": 942,
        "avg_rows_per_partition": 18_500,
        "metadata_verified_at": "2026-08-01T06:00:00Z",
    },
    "transactions": {
        "partition_column": "txn_date",
        "partition_type": "date",
        "earliest_partition": "2022-06-01",
        "latest_partition": "2026-07-31",
        "total_partitions": 1521,
        "avg_rows_per_partition": 42_000,
        "metadata_verified_at": "2026-08-01T06:00:00Z",
    },
    "sensor_readings": {
        "partition_column": "reading_ts",
        "partition_type": "timestamp_hour",
        "earliest_partition": "2025-01-01T00:00:00Z",
        "latest_partition": "2026-07-31T23:00:00Z",
        "total_partitions": 13_560,
        "avg_rows_per_partition": 3_200,
        "metadata_verified_at": "2026-08-01T06:00:00Z",
    },
    "customer_profiles": {
        "partition_column": None,
        "partition_type": "none",
        "total_rows": 5_600_000,
        "metadata_verified_at": "2026-08-01T06:00:00Z",
    },
    # "audit_log" intentionally absent — metadata not available
}

DOWNSTREAM_DEPENDENCIES = {
    "orders":           ["silver_orders", "gold_revenue_summary", "pbi_revenue_report"],
    "transactions":     ["silver_txn", "gold_finance_ledger"],
    "sensor_readings":  ["silver_iot_readings", "ml_anomaly_model"],
    "customer_profiles":["silver_customers"],
}


def get_table_partition_metadata(table_name):
    if table_name not in PARTITION_METADATA:
        return {
            "table": table_name,
            "status": "METADATA_UNAVAILABLE",
            "message": f"No partition metadata found for table '{table_name}'. Cannot verify extract scope.",
        }
    meta = PARTITION_METADATA[table_name].copy()
    meta["table"] = table_name
    meta["status"] = "OK"
    return meta


def validate_partition_filter(table_name, filter_column, filter_start, filter_end):
    meta = get_table_partition_metadata(table_name)
    if meta["status"] == "METADATA_UNAVAILABLE":
        return {"valid": False, "reason": "METADATA_UNAVAILABLE", "detail": meta["message"]}

    part_col = meta.get("partition_column")

    if part_col is None:
        if filter_column:
            return {
                "valid": False,
                "reason": "TABLE_NOT_PARTITIONED",
                "detail": f"Table '{table_name}' has no partition column. Filter on '{filter_column}' cannot prune partitions.",
            }
        return {
            "valid": True,
            "reason": "TABLE_NOT_PARTITIONED_NO_FILTER",
            "detail": f"Table '{table_name}' is unpartitioned; full extract is expected.",
            "estimated_rows": meta.get("total_rows", "unknown"),
        }

    if filter_column != part_col:
        return {
            "valid": False,
            "reason": "WRONG_PARTITION_COLUMN",
            "detail": f"Filter specifies column '{filter_column}' but '{table_name}' is partitioned on '{part_col}'. Filter will NOT prune partitions — full table scan risk.",
        }

    if meta["partition_type"] == "date":
        try:
            start_dt = datetime.date.fromisoformat(filter_start)
            end_dt = datetime.date.fromisoformat(filter_end)
        except ValueError as exc:
            return {"valid": False, "reason": "INVALID_DATE_FORMAT", "detail": str(exc)}

        if start_dt > end_dt:
            return {"valid": False, "reason": "INVERTED_FILTER_WINDOW", "detail": f"filter_start ({filter_start}) is after filter_end ({filter_end})."}

        earliest = datetime.date.fromisoformat(meta["earliest_partition"])
        latest = datetime.date.fromisoformat(meta["latest_partition"])

        if end_dt < earliest:
            return {"valid": False, "reason": "FILTER_BEFORE_DATA_EXISTS", "detail": f"filter_end ({filter_end}) is before earliest known partition ({meta['earliest_partition']}). Extract would return zero rows."}
        if start_dt > latest:
            return {"valid": False, "reason": "FILTER_BEYOND_LATEST_PARTITION", "detail": f"filter_start ({filter_start}) is after latest known partition ({meta['latest_partition']})."}

        window_days = (end_dt - start_dt).days + 1
        if window_days > 366:
            return {"valid": False, "reason": "SUSPICIOUSLY_WIDE_WINDOW", "detail": f"Filter window spans {window_days} days (>366). Likely accidental full-history extract."}

        return {
            "valid": True, "reason": "FILTER_VERIFIED",
            "detail": "Partition filter passes all checks.",
            "window_days": window_days,
            "estimated_rows": window_days * meta["avg_rows_per_partition"],
            "partition_column": part_col,
        }

    if meta["partition_type"] == "timestamp_hour":
        return {"valid": True, "reason": "FILTER_VERIFIED_TIMESTAMP", "detail": "Timestamp partition filter accepted.", "partition_column": part_col}

    return {"valid": False, "reason": "UNKNOWN_PARTITION_TYPE", "detail": meta["partition_type"]}


def check_no_filter_risk(table_name):
    meta = get_table_partition_metadata(table_name)
    deps = DOWNSTREAM_DEPENDENCIES.get(table_name, [])
    if meta["status"] == "METADATA_UNAVAILABLE":
        return {"risk": "UNKNOWN", "detail": f"Cannot assess full-scan risk: metadata unavailable for '{table_name}'.", "downstream": deps}
    part_col = meta.get("partition_column")
    if part_col is None:
        return {"risk": "ACCEPTABLE", "detail": f"'{table_name}' is unpartitioned — full extract is the only option.", "estimated_rows": meta.get("total_rows", "unknown"), "downstream": deps}
    total_partitions = meta.get("total_partitions", "unknown")
    avg_rows = meta.get("avg_rows_per_partition", 0)
    estimated_total = total_partitions * avg_rows if isinstance(total_partitions, int) and isinstance(avg_rows, int) else "unknown"
    return {
        "risk": "HIGH",
        "detail": f"No partition filter on '{table_name}' (partitioned by '{part_col}'). This will scan ALL {total_partitions} partitions (~{estimated_total:,} rows if numeric). Accidental full-table-scan.",
        "estimated_total_rows": estimated_total,
        "downstream": deps,
        "recommendation": "Provide a partition filter before proceeding.",
    }


def approve_extract(table_name, filter_summary, estimated_rows):
    return {"action": "APPROVED", "table": table_name, "filter_summary": filter_summary, "estimated_rows": estimated_rows, "message": "Partition filter verified. Extract may proceed."}

def block_extract(table_name, reason, detail):
    return {"action": "BLOCKED", "table": table_name, "reason": reason, "detail": detail, "message": "Extract blocked. Resolve partition filter issue before proceeding."}

def escalate_to_human(table_name, reason, detail):
    return {"action": "ESCALATED", "table": table_name, "reason": reason, "detail": detail, "message": "Partition scope cannot be verified. Human review required before extract."}


def run_agent(scenario):
    trace = []
    table = scenario["table"]
    filter_column = scenario.get("filter_column")
    filter_start = scenario.get("filter_start")
    filter_end = scenario.get("filter_end")

    meta_result = get_table_partition_metadata(table)
    trace.append({"tool": "get_table_partition_metadata", "result": meta_result})

    if not filter_column:
        risk_result = check_no_filter_risk(table)
        trace.append({"tool": "check_no_filter_risk", "result": risk_result})
        if meta_result["status"] == "METADATA_UNAVAILABLE":
            trace.append({"tool": "escalate_to_human", "result": escalate_to_human(table, "METADATA_UNAVAILABLE_NO_FILTER", "No filter provided and partition metadata is unavailable.")})
        elif risk_result["risk"] == "ACCEPTABLE":
            trace.append({"tool": "approve_extract", "result": approve_extract(table, "Full table (unpartitioned)", risk_result.get("estimated_rows", "unknown"))})
        else:
            trace.append({"tool": "block_extract", "result": block_extract(table, "NO_PARTITION_FILTER_ON_PARTITIONED_TABLE", risk_result["detail"])})
        return trace

    val_result = validate_partition_filter(table, filter_column, filter_start, filter_end)
    trace.append({"tool": "validate_partition_filter", "result": val_result})

    if meta_result["status"] == "METADATA_UNAVAILABLE":
        trace.append({"tool": "escalate_to_human", "result": escalate_to_human(table, "METADATA_UNAVAILABLE_FILTER_UNVERIFIABLE", f"Filter {filter_column} BETWEEN {filter_start} AND {filter_end} looks syntactically valid but partition metadata is unavailable — effective scope cannot be confirmed. Approving on syntax alone would violate the guardrail.")})
        return trace

    if val_result["valid"]:
        trace.append({"tool": "approve_extract", "result": approve_extract(table, f"{filter_column} BETWEEN {filter_start} AND {filter_end}", val_result.get("estimated_rows", "N/A"))})
    else:
        trace.append({"tool": "block_extract", "result": block_extract(table, val_result["reason"], val_result["detail"])})
    return trace


SCENARIOS = [
    {"id": 1, "name": "Valid incremental extract — correct column & window", "description": "Pipeline requests yesterday's orders using the correct partition column and a 1-day window within known boundaries. Should be APPROVED.", "table": "orders", "filter_column": "order_date", "filter_start": "2026-07-31", "filter_end": "2026-07-31"},
    {"id": 2, "name": "Wrong partition column — silent full-scan risk", "description": "Pipeline filters on 'created_at' but 'orders' is partitioned on 'order_date'. The filter will not prune any partitions. Should be BLOCKED.", "table": "orders", "filter_column": "created_at", "filter_start": "2026-07-31", "filter_end": "2026-07-31"},
    {"id": 3, "name": "No filter on partitioned table — transactions", "description": "Extract job submitted with no partition filter on 'transactions' table which has 1,521 date partitions (~63M rows). Should be BLOCKED.", "table": "transactions", "filter_column": None, "filter_start": None, "filter_end": None},
    {"id": 4, "name": "Table with no metadata — syntactically correct filter must still be escalated", "description": "'audit_log' table has no entry in the metadata store. Even though the filter looks valid on its face, the agent MUST escalate because it cannot verify effective scope. This is the core guardrail.", "table": "audit_log", "filter_column": "log_date", "filter_start": "2026-07-01", "filter_end": "2026-07-31"},
]


def print_trace(scenario, trace):
    print("=" * 70)
    print(f"SCENARIO {scenario['id']}: {scenario['name']}")
    print(f"Table   : {scenario['table']}")
    print(f"Filter  : {scenario.get('filter_column')} [{scenario.get('filter_start')} -> {scenario.get('filter_end')}]")
    print(f"\n{scenario['description']}\n")
    for step in trace:
        print(f"  -> TOOL: {step['tool']}")
        result = step["result"]
        for key in ["status", "valid", "reason", "risk", "action", "detail", "estimated_rows", "estimated_total_rows", "message"]:
            if key in result:
                val = result[key]
                if isinstance(val, int):
                    val = f"{val:,}"
                print(f"       {key:26s}: {val}")
    print()


if __name__ == "__main__":
    print("\nDay 12 — ETL Partition Pruning Verification Agent\n")
    for scenario in SCENARIOS:
        trace = run_agent(scenario)
        print_trace(scenario, trace)
