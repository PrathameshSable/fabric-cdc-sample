# SQL Server CDC → Fabric Lakehouse: metadata-driven pipeline

*Author: Prathamesh Sable*

> **Running more than ~20 tables?** Use batch mode: see `README_batch_mode.md`. It reuses everything here and adds a single-call planner, bulk watermarking, and one Spark session for all tables. Keep `pl_cdc_table` for initial loads of very large tables.

## Components

| # | File | Where it runs |
|---|------|---------------|
| 1 | `01_source_sqlserver_cdc_setup.sql` | On-prem SQL Server (CDC enable, permissions, retention, health checks) |
| 2 | `02_control_db_objects.sql` | **SQL database in Fabric** (control table, run log, procs) |
| 3 | `03_pipeline_source_queries.sql` | Pasted into the pipeline Lookup / Copy activities |
| 4 | `nb_cdc_apply.ipynb` (+ `.py` source) | Fabric notebook, default lakehouse = your silver lakehouse |
| 5 | This README | Pipeline build: `pl_cdc_orchestrator` + `pl_cdc_table` |

```
pl_cdc_orchestrator
 ├─ Lookup_Control  (Fabric SQL DB → etl.cdc_control)
 └─ ForEach_Table  (parallel, batch 4)
     └─ Invoke pl_cdc_table
          ├─ Lookup_LSN_Window (SQL Server via gateway)  → load_action + LSN window
          ├─ Set_LandingPath
          ├─ Switch_LoadAction
          │    FULL        : Copy_Full → NB_Apply_Full → SP_Complete_Full
          │    INCREMENTAL : Copy_Incr → NB_Apply_Incr → SP_Complete_Incr
          │    NOCHANGE    : SP_Complete_NoChange   (advances watermark only)
          │    Default     : Fail (NO_CAPTURE_INSTANCE)
          ├─ on Lookup fail : SP_LogFail_Lookup → Fail_Rethrow_1
          └─ on Switch fail : SP_LogFail_Switch → Fail_Rethrow_2
```

## Why the design is built this way

**The watermark moves only after the MERGE succeeds.** Any failure leaves `last_lsn_hex` unchanged, so the next run retries the same window. The notebook is idempotent on re-runs: the SCD1 MERGE uses an LSN guard, and SCD2 checks `_cdc_lsn` before closing or inserting a version.

**The control store is a Fabric SQL database, not a Warehouse.** Warehouse detects write conflicts at table level, so parallel iterations that update the same control table would fail each other.

**`to_lsn` is the database max LSN, read before anything else.** For a table with no changes, the watermark still advances (NOCHANGE). Without this, a quiet table's watermark would eventually fall behind the cleanup job and trigger a needless full reload.

**The initial snapshot is stamped with `to_lsn`, which is captured before the copy starts.** Changes committed while the snapshot is running get re-applied by the next incremental. That is safe because the apply is upsert/delete by key.

**Reloads never wipe SCD2 history.** A FULL load into an existing SCD2 table is diffed against the current rows using a hash and turned into synthetic changes: upserts for new or changed keys, deletes for missing keys.

---

## 1. Connections

| Name | Type | Used by |
|---|---|---|
| `cn_sql_src_erp` | SQL Server, on-premises data gateway, `svc_fabric_ingest` | Lookup_LSN_Window, Copy_* |
| `cn_fabric_sqldb_ctrl` | SQL database (Fabric) | Lookup_Control, SP_* |
| Lakehouse `lh_silver` | Workspace item | Copy sink (Files), notebook default lakehouse |

## 2. `pl_cdc_orchestrator`

**Parameters:** `load_group` (String, default `default`)

**Lookup_Control**: connection `cn_fabric_sqldb_ctrl`, *First row only* **off**
```sql
SELECT control_id, source_system, source_schema, source_table, capture_instance,
       key_columns, target_table, scd_type, ISNULL(last_lsn_hex, '') AS last_lsn_hex
FROM   etl.cdc_control
WHERE  is_active = 1 AND load_group = '@{pipeline().parameters.load_group}'
ORDER BY control_id;
```

**ForEach_Table**
- Items: `@activity('Lookup_Control').output.value`
- Sequential: **off**, Batch count: **4**. Size this to what the gateway can handle; it is usually the bottleneck.

Inside the ForEach, add **Invoke pipeline → `pl_cdc_table`** with *Wait on completion* **on**:

| Parameter | Type | Value |
|---|---|---|
| control_id | Int | `@item().control_id` |
| source_system | String | `@item().source_system` |
| source_schema | String | `@item().source_schema` |
| source_table | String | `@item().source_table` |
| capture_instance | String | `@item().capture_instance` |
| key_columns | String | `@item().key_columns` |
| target_table | String | `@item().target_table` |
| scd_type | Int | `@item().scd_type` |
| last_lsn_hex | String | `@item().last_lsn_hex` |

The ForEach keeps processing the other tables when one fails, and the orchestrator ends as *Failed*, so your alerting still fires.

## 3. `pl_cdc_table`

**Parameters:** the nine above. **Variables:** `landing_path` (String).

### 3.1 Lookup_LSN_Window
- Connection `cn_sql_src_erp`, *Use query* → Query, *First row only* **on**
- Query: block **A** from `03_pipeline_source_queries.sql`, entered as dynamic content
- Returns `load_action`, `reason`, `from_lsn_hex`, `to_lsn_hex`, `min_lsn_hex`

### 3.2 Set_LandingPath (Set variable, on Lookup success)
```
@concat('cdc_landing/', pipeline().parameters.source_system, '/',
        pipeline().parameters.capture_instance, '/', pipeline().RunId)
```

### 3.3 Switch_LoadAction (on Set_LandingPath success)
Expression: `@activity('Lookup_LSN_Window').output.firstRow.load_action`

#### Case `FULL`

**Copy_Full**
- Source: `cn_sql_src_erp`, Query = block **C**. Set *Isolation level* = Snapshot if `ALLOW_SNAPSHOT_ISOLATION` is on; for large tables, set *Partition option* = Dynamic range on the PK.
- Destination: Lakehouse `lh_silver`, Root folder **Files**, Folder path `@variables('landing_path')`, File name `data.parquet`, Format **Parquet**.

**NB_Apply_Full** (Notebook `nb_cdc_apply`, on Copy_Full success). Base parameters:

| Name | Type | Value |
|---|---|---|
| target_table | String | `@pipeline().parameters.target_table` |
| key_columns | String | `@pipeline().parameters.key_columns` |
| landing_path | String | `@concat('Files/', variables('landing_path'))` |
| load_action | String | `FULL` |
| scd_type | Int | `@pipeline().parameters.scd_type` |
| to_lsn_hex | String | `@activity('Lookup_LSN_Window').output.firstRow.to_lsn_hex` |
| rows_copied | Int | `@activity('Copy_Full').output.rowsCopied` |
| run_id | String | `@pipeline().RunId` |

**SP_Complete_Full** (Stored procedure `etl.usp_cdc_complete` on `cn_fabric_sqldb_ctrl`, on NB success):

| Param | Value |
|---|---|
| control_id | `@pipeline().parameters.control_id` |
| pipeline_run_id | `@pipeline().RunId` |
| load_action | `FULL` |
| from_lsn_hex | `@coalesce(activity('Lookup_LSN_Window').output.firstRow.from_lsn_hex, '')` |
| to_lsn_hex | `@activity('Lookup_LSN_Window').output.firstRow.to_lsn_hex` |
| rows_copied | `@activity('Copy_Full').output.rowsCopied` |
| apply_result | `@string(activity('NB_Apply_Full').output.result.exitValue)` |

#### Case `INCREMENTAL`
This case mirrors FULL with three differences: **Copy_Incr** uses Query block **B**, the notebook gets `load_action = INCREMENTAL` and `rows_copied = @activity('Copy_Incr').output.rowsCopied`, and the SP call passes `load_action = INCREMENTAL`.

#### Case `NOCHANGE`
**SP_Complete_NoChange** runs `etl.usp_cdc_complete` with `load_action = NOCHANGE`, `rows_copied = 0`, and `to_lsn_hex` taken from the Lookup. It does no copy and starts no Spark session.

#### Default
**Fail** activity with message
`@activity('Lookup_LSN_Window').output.firstRow.reason`, error code `CDC_CONFIG`.

### 3.4 Failure handling
- **SP_LogFail_Lookup** runs on Lookup_LSN_Window **Failed** and calls `etl.usp_cdc_log_failure` with `error_message = @activity('Lookup_LSN_Window').error.message`.
- **SP_LogFail_Switch** runs on Switch_LoadAction **Failed** and passes `error_message = @activity('Switch_LoadAction').error.message` plus the `load_action`, `from_lsn_hex` and `to_lsn_hex` from the Lookup.
- After each of these, add a **Fail** activity to re-throw the error. Once the failure branch succeeds, the pipeline would otherwise report **Succeeded** and your alerts would never fire.
- Use **two separate** log activities. A single activity that has two *Failed* dependencies uses AND logic, so it would never run.

## 4. Notebook setup
1. Import `nb_cdc_apply.ipynb` and attach `lh_silver` (schema-enabled) as the default lakehouse.
2. Confirm the first cell is marked as the **parameter cell** by toggling it in the cell menu.
3. In the workspace Spark settings, enable **high concurrency for pipelines**. With 4+ parallel tables this saves a session start-up per table. The notebook uses a uniquely named temp view, so shared sessions are safe.

### What the notebook writes

| SCD | Target columns added |
|---|---|
| 1 | `_cdc_lsn`, `_loaded_at`, `_run_id` |
| 2 | `_valid_from`, `_valid_to`, `_is_current`, `_is_deleted`, `_cdc_lsn`, `_loaded_at`, `_run_id` |

The exit value is a JSON string with the mode, rows in, Delta merge metrics, and any columns added. It is stored in `etl.cdc_run_log.apply_result`.

## 5. Operating runbook

**Onboard a table.** Enable CDC on the source (script 01), insert a row into `etl.cdc_control`, and the next run performs the FULL load.

**Force a reload.** Run `EXEC etl.usp_cdc_reset @control_id = n;`. SCD1 tables are overwritten; SCD2 tables are diffed so history is kept.

**Source schema change (add a column).** A capture instance doesn't pick up new columns.
1. Add the column on the source.
2. Create a second instance, `dbo_Customer_v2` (a table can have at most 2 capture instances).
3. Wait until the table's `last_lsn_hex` is ≥ `start_lsn` of v2 (see `cdc.change_tables`).
4. `UPDATE etl.cdc_control SET capture_instance = 'dbo_Customer_v2' WHERE control_id = n;`
5. On the next run, the notebook adds the new column to the Delta table (`ALTER TABLE ADD COLUMNS`).
6. Disable v1 with `sys.sp_cdc_disable_table`.

If you switch before step 3 is satisfied, the Lookup sees the watermark as older than v2's min LSN and automatically runs a FULL load. That is safe, just costlier.

**Monitoring.** `SELECT * FROM etl.vw_cdc_status;` covers the Fabric side. On the source, the health checks in script 01 show capture latency and errors, and confirm SQL Agent is running.

**Housekeeping.** Landing folders under `Files/cdc_landing/` are kept for audit. Add a weekly notebook to delete anything older than N days, and schedule `OPTIMIZE` / `VACUUM` on high-churn targets.

## 6. Known limits and checks before go-live
- **Capture all columns** (`@captured_column_list = NULL`). The snapshot uses `SELECT *`, so a partial capture list causes drift between full and incremental loads.
- **Unsupported column types.** Columns such as `sql_variant`, `xml`, `geography`, `geometry` and `hierarchyid` don't land cleanly in Parquet. Replace `c.*` / `t.*` with an explicit column list that casts them.
- **Column names** with spaces or `,;{}()=` are not valid in Delta without column mapping. Alias them in the source query.
- **`cdc_commit_time`** comes from `cdc.lsn_time_mapping.tran_end_time`, which is **source-server local time**. Convert it if you need UTC in `_valid_from`/`_valid_to`.
- **SCD2 granularity is per batch.** Net changes collapse multiple updates to one key within a window into one version. If you need every intermediate version, switch block B to `fn_cdc_get_all_changes_… (..., N'all')`, keep only `__$operation IN (1,2,4)`, and apply the versions in `__$seqval` order.
- **Avoid overlapping orchestrator runs.** Keep the trigger interval longer than the worst-case runtime. Overlap won't corrupt data, because the apply is idempotent and the watermark only moves forward, but it wastes capacity.
- **CDC retention.** Keep the source retention (7 days in script 01) comfortably above your longest expected outage.
- **Testing.** The notebook logic has been desk-checked but not executed against a live lakehouse. Run the test matrix below in DEV before promoting:
  - initial load
  - insert, update and delete
  - a re-run of the same window after a failure
  - a forced reload on SCD2
  - adding a column through a capture-instance switch
