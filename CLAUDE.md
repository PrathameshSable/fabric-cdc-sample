# Project: SQL Server CDC → Microsoft Fabric (metadata-driven, ~150 tables)

Owner: Prathamesh Sable. Credit "Prathamesh Sable" in any authored content.

## Goal
Deploy and test the CDC ingestion framework in this folder into Fabric workspace
`fb55593b-880f-415c-8732-ea277588d9ef`, starting in DEV.

## What's already built (in this folder)
| File | Purpose |
|---|---|
| 01_source_sqlserver_cdc_setup.sql | Source: CDC enable, gating role, retention, health checks |
| 02_control_db_objects.sql | Fabric SQL DB: etl.cdc_control, etl.cdc_run_log, usp_cdc_complete / log_failure / reset, vw_cdc_status |
| 03_pipeline_source_queries.sql | Per-table Lookup + Copy queries (pl_cdc_table) |
| 04_bulk_register_from_source.sql | Generates control-table INSERTs from cdc.change_tables, flags problem tables |
| 05_batch_planner_query.sql | One Lookup that plans all tables (action, LSN window, copy_query) |
| 06_control_db_batch_objects.sql | usp_cdc_complete_batch, usp_cdc_activate_wave |
| nb_cdc_apply.ipynb / .py | Per-table apply: SCD1 MERGE / SCD2 history, idempotent via _cdc_lsn, schema evolution |
| nb_cdc_apply_batch.ipynb / .py | Runs nb_cdc_apply per table on a thread pool in one Spark session |
| README.md | Per-table pipeline (pl_cdc_table) build guide |
| README_batch_mode.md | Batch pipeline (pl_cdc_batch) build guide, sizing, runbook |

## Key design decisions (don't change without discussion)
- Control store = **SQL database in Fabric**, not Warehouse (table-level write conflicts break parallel watermark updates).
- Watermark (`last_lsn_hex`, '0x'+20 hex) advances **only after a successful apply**; forward-only.
- `to_lsn` = DB max LSN read first; NOCHANGE tables still advance watermark so they never age past CDC retention.
- Net changes with `'all with merge'` (op 1 = delete, 5 = upsert). SCD2 history is per-batch granularity.
- Initial snapshot stamped with to_lsn captured before the copy; re-applied overlap is safe (idempotent MERGE).
- SCD2 reloads diff the snapshot against current rows (hash) — history is never overwritten.
- Tables registered inactive; onboard in waves of ~25 (`usp_cdc_activate_wave`). Tables > 50M rows → load_group 'large', initial load via pl_cdc_table with partitioned copy.

## Next steps for Claude Code
1. Authenticate (`az login`) and confirm access to the workspace via Fabric REST (`--resource https://api.fabric.microsoft.com`).
2. Inventory the workspace: existing lakehouses, SQL databases, notebooks, pipelines, connections, gateway.
3. Create (if missing): schema-enabled lakehouse for silver, Fabric SQL database for control; run 02 + 06 scripts.
4. Upload nb_cdc_apply and nb_cdc_apply_batch; attach the lakehouse as default; verify parameter cells.
5. Build pl_cdc_batch (and pl_cdc_table) from README_batch_mode.md / README.md as pipeline definitions via REST.
6. Source side (needs user/DBA): run 01 and 04; fix flagged tables; load control rows.
7. DEV test matrix: initial load, insert/update/delete, re-run after failure, forced SCD2 reload,
   capture-instance switch, one deliberately broken table (failure isolation). Validate the planner in SSMS first.

## Untested items to verify first
- nb_cdc_apply Delta MERGE logic was desk-checked, not run.
- nb_cdc_apply_batch: confirm `notebookutils.notebook.run` from threads shares one session (fallback: `runMultiple`).
- Pipeline expression details (Filter `.output.value`, notebook `output.result.exitValue`) — confirm in the target runtime.
