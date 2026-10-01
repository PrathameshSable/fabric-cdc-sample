# Scaling to 150+ tables: batch mode (`pl_cdc_batch`)

*Author: Prathamesh Sable*

The per-table pipeline (`pl_cdc_table`, see README.md) is fine for a handful of tables. At around 150 tables, three costs dominate:

| Cost | Per-table pattern | Batch pattern |
|---|---|---|
| Round-trips to the source to plan | 150 Lookups | **1** Lookup (`05_batch_planner_query.sql`) |
| Spark session starts | up to 150 | **1** (all tables applied in one session) |
| Watermark / log calls | ~150 SP calls | **2** (`usp_cdc_complete_batch`) |
| Tables with no changes | Lookup + SP each | **zero copy, zero Spark**, watermark advanced in bulk |

Copy stays per table, because that is where the parallelism belongs.

## Files for batch mode

| File | Where |
|---|---|
| `04_bulk_register_from_source.sql` | Source. Generates the 150 `INSERT`s for `etl.cdc_control` and flags broken tables |
| `05_batch_planner_query.sql` | Pasted into `Lookup_Plan` |
| `06_control_db_batch_objects.sql` | Fabric SQL DB. Adds `usp_cdc_complete_batch` and `usp_cdc_activate_wave` |
| `nb_cdc_apply_batch.ipynb` | Fabric notebook. Runs `nb_cdc_apply` per table on a thread pool inside one session |

`nb_cdc_apply`, the control tables and script 01 are unchanged from v1.

```
pl_cdc_batch (param: load_group)
 ├─ Lookup_Control      Fabric SQL DB  → control_json (1 row)
 ├─ Lookup_Plan         SQL Server     → 1 row per table: action, LSNs, landing_path, copy_query
 ├─ Filter_Skip         NOCHANGE + NO_CAPTURE_INSTANCE
 │    └─ SP_Complete_Skipped   usp_cdc_complete_batch(Filter_Skip.value)
 ├─ Filter_NoCI         NO_CAPTURE_INSTANCE (for the final failure check)
 ├─ Filter_Work         FULL + INCREMENTAL
 │    └─ ForEach_Copy (parallel, batch N)
 │         Copy_Table (query = item().copy_query)
 │           ├─ success → Append_Copied      "control_id:rowsCopied"
 │           └─ failure → SP_LogFail_Copy → Append_CopyFailed
 ├─ If_AnyCopied  (after ForEach_Copy: Completed)
 │    true: NB_Apply_Batch → SP_Complete_Batch → Set_ApplyFailed
 ├─ Fail_ApplyStage (If_AnyCopied: Failed)
 └─ If_AnyFailures (after If_AnyCopied: Succeeded AND SP_Complete_Skipped: Succeeded)
      true: Fail "CDC batch finished with failures, see etl.vw_cdc_status"
```

## 1. Onboard the 150 tables

1. Run `04_bulk_register_from_source.sql` on the source and fix every row with something in the **issues** column before going further. The usual problems are:
   - no net-change support (no PK or unique index)
   - partial column capture
   - unsupported data types
   - column names with spaces
2. Add the gateway user to **every gating role** the script lists.
3. Run the `generated_sql` column in the Fabric SQL DB. All rows land with `is_active = 0`. Tables over 50M rows go to `load_group = 'large'`.
4. Run `06_control_db_batch_objects.sql`.
5. **Activate in waves.** Every newly activated table does a FULL load on its first run, so switching on 150 at once hits the source, the gateway and capacity together. Run `EXEC etl.usp_cdc_activate_wave @load_group = 'default', @wave_size = 25;`, let a run complete, then activate the next wave.
6. **Large tables.** For the handful in `large`, do the initial load with the v1 `pl_cdc_table` pipeline with the Copy *Partition option* set to Dynamic range by hand. Once they have a watermark, move them to their normal load group, and incrementals then run through batch mode like everything else.

## 2. Build `pl_cdc_batch`

**Parameters:** `load_group` (String), `apply_concurrency` (Int, default 8)
**Variables:** `copied` (Array), `copy_failed` (Array), `apply_failed` (String, default `0`)

### Lookup_Control
Connection: Fabric SQL DB, *First row only* **on**.
```sql
SELECT ISNULL((
    SELECT control_id, source_system, source_schema, source_table, capture_instance,
           key_columns, target_table, scd_type, ISNULL(last_lsn_hex, '') AS last_lsn_hex
    FROM   etl.cdc_control
    WHERE  is_active = 1 AND load_group = '@{pipeline().parameters.load_group}'
    FOR JSON PATH), '[]') AS control_json;
```
The outer `SELECT (… FOR JSON PATH)` wrapper matters. Without it, SQL splits the JSON across multiple 2,033-character rows and the Lookup gets fragments.

### Lookup_Plan
Connection: source SQL Server via the gateway, *First row only* **off**, Query = the whole of `05_batch_planner_query.sql`. The planner reads the database max LSN once, then does one index seek per table on its `_CT` table. For 150 tables this is seconds of work.

### Filters (all on Lookup_Plan success)

| Activity | Items | Condition |
|---|---|---|
| Filter_Skip | `@activity('Lookup_Plan').output.value` | `@or(equals(item().load_action,'NOCHANGE'), equals(item().load_action,'NO_CAPTURE_INSTANCE'))` |
| Filter_NoCI | same | `@equals(item().load_action,'NO_CAPTURE_INSTANCE')` |
| Filter_Work | same | `@or(equals(item().load_action,'FULL'), equals(item().load_action,'INCREMENTAL'))` |

### SP_Complete_Skipped (on Filter_Skip success)
Runs `etl.usp_cdc_complete_batch` with:
- `pipeline_run_id` = `@pipeline().RunId`
- `results_json` = `@string(activity('Filter_Skip').output.value)`

The planner already put `status` on each row: Succeeded for NOCHANGE, Failed for a missing capture instance. So this call advances the quiet tables' watermarks and logs the misconfigured ones.

### ForEach_Copy (on Filter_Work success)
- Items: `@activity('Filter_Work').output.value`
- Sequential **off**, Batch count **16** (see sizing below)

Inside the ForEach:

**Copy_Table**
- Source: SQL Server connection, Query = `@item().copy_query`, Isolation level = Snapshot if the database allows it, Query timeout 02:00:00
- Destination: Lakehouse, Root **Files**, Folder `@item().landing_path`, File `data.parquet`, Format Parquet

**Append_Copied** (on Copy_Table success), variable `copied`:
`@concat(string(item().control_id), ':', string(activity('Copy_Table').output.rowsCopied))`

**SP_LogFail_Copy** (on Copy_Table failure) runs `etl.usp_cdc_log_failure` with:
- `control_id` = `@item().control_id`
- `load_action` = `@item().load_action`
- `from_lsn_hex` / `to_lsn_hex` = `@item().from_lsn_hex` / `@item().to_lsn_hex`
- `error_message` = `@activity('Copy_Table').error.message`

**Append_CopyFailed** (on SP_LogFail_Copy success), variable `copy_failed`: `@string(item().control_id)`

Append Variable is safe inside a parallel ForEach. Set Variable is **not**, which is why the variables are arrays here.

### If_AnyCopied (on ForEach_Copy **Completed**)
Expression: `@greater(length(variables('copied')), 0)`

The dependency must be *Completed*, not *Succeeded*. A single failed copy marks the ForEach as failed, and the tables that copied fine still need to be applied.

The True branch has three activities:

**NB_Apply_Batch** runs notebook `nb_cdc_apply_batch` with these parameters:

| Name | Type | Value |
|---|---|---|
| plan_json | String | `@string(activity('Filter_Work').output.value)` |
| copied | String | `@join(variables('copied'), ',')` |
| run_id | String | `@pipeline().RunId` |
| concurrency | Int | `@pipeline().parameters.apply_concurrency` |
| child_notebook | String | `nb_cdc_apply` |
| table_timeout | Int | `3600` |

**SP_Complete_Batch** (on NB success) runs `etl.usp_cdc_complete_batch` with:
- `pipeline_run_id` = `@pipeline().RunId`
- `results_json` = `@activity('NB_Apply_Batch').output.result.exitValue`

**Set_ApplyFailed** (on SP success), variable `apply_failed`:
`@string(json(activity('NB_Apply_Batch').output.result.exitValue).failed_count)`

### Fail_ApplyStage (on If_AnyCopied **Failed**)
This is a Fail activity with the message "Apply stage failed (notebook or batch completion); watermarks not advanced for copied tables".

### If_AnyFailures
Depends on If_AnyCopied **Succeeded** and SP_Complete_Skipped **Succeeded**.
```
@or(or(greater(length(variables('copy_failed')), 0),
       greater(int(variables('apply_failed')), 0)),
    greater(activity('Filter_NoCI').output.FilteredItemsCount, 0))
```
If true, a Fail activity reports the batch as failed. Every per-table failure is already logged at this point, and all other tables have been completed, so this only makes the pipeline status red and triggers alerts.

## 3. Sizing for 150 tables

**Gateway (ForEach batch count).** The gateway is the usual bottleneck. Start at a batch count of 8–16 per gateway node and watch the gateway's CPU and memory. A 2–3 node gateway cluster gives both throughput and high availability. The ForEach maximum is 50.

**Source.** Incremental copies are light. FULL copies are the heavy part, which is why tables are activated in waves and why `large` tables get their initial load separately.

**Spark (`apply_concurrency`).** Every table's MERGE runs in the same session. On a Medium pool with autoscale, 8 is a safe start. Raise it if executors sit idle; lower it if you see driver memory pressure. The batch notebook sorts tables by rows copied, largest first, so big tables don't end up waiting at the tail.

**Schedules.** Create one trigger per `load_group`. A common split: `hot`, for high-change fact tables, every 15–30 min; `default` hourly; `reference` daily. Moving a table between groups is a single UPDATE on the control table.

**Overlap.** Keep each group's trigger interval longer than its worst run time. Two groups can run concurrently because each table belongs to one group only.

## 4. Watching 150 tables
```sql
-- Anything failing or stale
SELECT * FROM etl.vw_cdc_status
WHERE  last_run_status = 'Failed'
   OR  last_success_utc < DATEADD(hour, -6, SYSUTCDATETIME())
ORDER BY last_success_utc;

-- Slowest / biggest tables in the last run (candidates for their own group)
SELECT TOP (20) c.source_table, l.load_action, l.rows_copied,
       JSON_VALUE(l.apply_result, '$.rows_in') AS rows_in, l.logged_utc
FROM   etl.cdc_run_log l JOIN etl.cdc_control c ON c.control_id = l.control_id
WHERE  l.pipeline_run_id = '<run id>'
ORDER BY l.rows_copied DESC;
```
Also check the source health queries in script 01. At 150 tables, the CDC **capture job** latency matters more than anything on the Fabric side.

## 5. Behaviour on partial failure

| What fails | Effect |
|---|---|
| One Copy | Logged; that table's watermark stays put and it retries next run. All others are applied. |
| One table's MERGE | Isolated inside the batch notebook; logged; watermark stays. Others complete. |
| The whole notebook (e.g. session start) | Nothing applied, no watermark moves, pipeline fails. The next run re-copies and re-applies safely. |
| Planner Lookup | Nothing runs; pipeline fails. |

Nothing is lost in any of these cases: the watermark only moves after a successful apply, and the apply is idempotent.

## 6. Before go-live
- **Nested notebook runs.** `nb_cdc_apply_batch` calls child notebooks via `notebookutils.notebook.run` from a thread pool, and the thread-pool logic has been tested with a stub. Confirm on your runtime that child runs share the session and that `concurrency` behaves as expected. `notebookutils.notebook.runMultiple` is the built-in alternative if you prefer a DAG.
- **Planner query.** Run it once in SSMS with a hand-pasted `control_json` before wiring it into the pipeline.
- **Soak test.** Before activating everything, run one wave of about 25 tables through several cycles and cover each of these:
  - inserts, updates and deletes
  - a forced reload with `usp_cdc_reset`
  - one deliberately broken table, to confirm the failure isolation
