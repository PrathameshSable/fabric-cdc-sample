# %% [parameters]
# ---------------------------------------------------------------------
# nb_cdc_apply_batch  -  applies all copied tables in ONE Spark session
# Runs nb_cdc_apply per table on a thread pool (same session, no
# per-table session start). One table failing never stops the others.
# Author: Prathamesh Sable
# ---------------------------------------------------------------------
plan_json      = "[]"            # Filter_Work output (planner rows)
copied         = ""              # "control_id:rows_copied,..." from Append Variable
run_id         = "manual"
concurrency    = 8               # parallel tables; size to the Spark pool
child_notebook = "nb_cdc_apply"
table_timeout  = 3600            # seconds per table

# %%
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

concurrency   = int(concurrency)
table_timeout = int(table_timeout)

plan = {int(p["control_id"]): p for p in json.loads(plan_json or "[]")}

copied_rows = {}
for token in (copied or "").split(","):
    token = token.strip()
    if token:
        cid, rows = token.split(":", 1)
        copied_rows[int(cid)] = int(rows or 0)

print(f"Plan rows: {len(plan)}  |  copied OK: {len(copied_rows)}  |  concurrency: {concurrency}")


def base_result(cid, p, rows, status, **extra):
    return {"control_id": cid,
            "load_action": p.get("load_action") if p else None,
            "from_lsn_hex": (p.get("from_lsn_hex") or "") if p else "",
            "to_lsn_hex": p.get("to_lsn_hex") if p else None,
            "rows_copied": rows,
            "status": status,
            **extra}


def apply_one(cid, rows):
    p = plan.get(cid)
    if p is None:
        return base_result(cid, None, rows, "Failed", error_message="control_id not in plan")
    args = {
        "target_table": p["target_table"],
        "key_columns":  p["key_columns"],
        "landing_path": "Files/" + p["landing_path"],
        "load_action":  p["load_action"],
        "scd_type":     int(p["scd_type"]),
        "to_lsn_hex":   p["to_lsn_hex"],
        "rows_copied":  rows,
        "run_id":       run_id,
    }
    t0 = time.time()
    try:
        exit_val = notebookutils.notebook.run(child_notebook, table_timeout, args)
        return base_result(cid, p, rows, "Succeeded",
                           apply_result=exit_val, seconds=round(time.time() - t0, 1))
    except Exception as e:  # isolate per-table failures
        return base_result(cid, p, rows, "Failed",
                           error_message=str(e)[:3900], seconds=round(time.time() - t0, 1))

# %%
# Biggest batches first => better packing across the pool
work = sorted(copied_rows.items(), key=lambda kv: kv[1], reverse=True)
results = []

with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
    futures = {pool.submit(apply_one, cid, rows): cid for cid, rows in work}
    for f in as_completed(futures):
        r = f.result()
        results.append(r)
        print(f"[{r['status']:<9}] control_id={r['control_id']:<5} "
              f"{r.get('load_action') or '':<11} rows={r['rows_copied']:<10} {r.get('seconds', '')}s "
              f"{r.get('error_message', '')[:200]}")

summary = {
    "run_id": run_id,
    "tables": len(results),
    "succeeded": sum(r["status"] == "Succeeded" for r in results),
    "failed_count": sum(r["status"] != "Succeeded" for r in results),
    "results": sorted(results, key=lambda r: r["control_id"]),
}
print(json.dumps({k: v for k, v in summary.items() if k != "results"}, indent=2))

# %%
notebookutils.notebook.exit(json.dumps(summary, default=str))
