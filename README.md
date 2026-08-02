# Day 12 — ETL Partition Pruning Verification Agent

**Series:** Agentic AI in Data Engineering — ETL Edition  
**Pattern:** Pre-extract guardrail — verify partition filters *before* any bulk extract begins  
**Core guardrail:** Never approve an extract whose effective partition scope is unverifiable from available metadata, even if the filter looks syntactically correct.

---

## The Problem

In ETL pipelines, partition filters are the primary mechanism to avoid expensive full-table scans during the Extract stage. A filter that *looks* correct but references the wrong column, spans an invalid date range, or targets a table with missing metadata can silently cause:

- Accidental full-table scans consuming 100× the expected compute
- Downstream Silver/Gold tables receiving incorrect or incomplete data
- Cascading failures in BI reports and ML models

The standard approach is to let the extract job start and fail (or silently over-extract). This agent intercepts **before** data moves.

---

## What the Agent Does

For each pending extract request, the agent:

1. **Fetches partition metadata** for the source table  
2. **Checks for missing filters** — partitioned tables with no filter are blocked immediately  
3. **Validates the supplied filter** against known metadata:
   - Filter column must match the actual partition column
   - Date window must fall within known partition boundaries  
   - Window must not be inverted (start > end)  
   - Window must not be suspiciously wide (>366 days for daily partitions)
4. **Approves, blocks, or escalates** based on what can be confirmed

---

## Core Guardrail

> A filter that is syntactically correct is **not** sufficient to approve an extract.  
> If partition metadata is unavailable, the agent **must escalate** — never approve on syntax alone.

This distinguishes Day 12 from simple schema validation. Even a perfectly formed filter like `log_date BETWEEN '2026-07-01' AND '2026-07-31'` is blocked if the agent cannot confirm that `log_date` is the actual partition column and the window is within known data bounds.

---

## Scenarios

| # | Table | Filter | Expected Outcome |
|---|-------|--------|-----------------|
| 1 | `orders` | `order_date` 2026-07-31 (correct column, valid window) | ✅ APPROVED — 18,500 est. rows |
| 2 | `orders` | `created_at` 2026-07-31 (wrong partition column) | 🚫 BLOCKED — WRONG_PARTITION_COLUMN |
| 3 | `transactions` | No filter (1,521 partitions, ~63M rows) | 🚫 BLOCKED — NO_PARTITION_FILTER |
| 4 | `audit_log` | `log_date` 2026-07-01→07-31 (metadata absent) | ⚠️ ESCALATED — METADATA_UNAVAILABLE |

---

## Project Structure

```
etl-partition-pruning-agent/
├── agent.py          # Core agent: tools + agentic loop + 4 scenarios
├── requirements.txt  # No external deps (stdlib only)
├── .gitignore
├── LICENSE
└── README.md
```

---

## How to Run

```bash
pip install -r requirements.txt   # nothing to install
python agent.py
```

---

## New Agentic Pattern vs Prior Days

| Day | Stage | Pattern |
|-----|-------|---------|
| 10 | Load | Row-count reconciliation after load |
| 11 | Extract | Schema drift detection (column add/remove/type change) |
| **12** | **Extract** | **Partition filter verification before extract begins** |

Day 11 asks: *"Did the source schema change?"*  
Day 12 asks: *"Will this extract actually scan what we think it will scan?"*

---

## License

MIT
