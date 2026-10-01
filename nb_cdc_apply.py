# %% [parameters]
# ---------------------------------------------------------------------
# nb_cdc_apply  -  applies one table's CDC batch to a Lakehouse Delta table
# Called by pl_cdc_table (Notebook activity). Author: Prathamesh Sable
# ---------------------------------------------------------------------
target_table = "silver.customer"            # lakehouse table (schema.table)
key_columns  = "CustomerID"                 # comma-separated business key
landing_path = "Files/cdc_landing/ERP_PROD/dbo_Customer/manual"
load_action  = "INCREMENTAL"                # FULL | INCREMENTAL
scd_type     = 1                            # 1 = overwrite, 2 = history
to_lsn_hex   = ""                           # LSN the batch is consistent to
rows_copied  = -1                           # from Copy activity; -1 = unknown
run_id       = "manual"                     # pipeline run id, stamped on rows

# %%
import json
import uuid
from pyspark.sql import functions as F, Window
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

CDC_META    = ["cdc_lsn", "cdc_operation", "cdc_commit_time"]
SCD2_COLS   = ["_valid_from", "_valid_to", "_is_current", "_is_deleted"]
AUDIT_COLS  = ["_cdc_lsn", "_loaded_at", "_run_id"]
SYSTEM_COLS = {c.lower() for c in CDC_META + SCD2_COLS + AUDIT_COLS}
SRC_VIEW    = f"cdc_src_{uuid.uuid4().hex[:12]}"   # unique: safe under high-concurrency sessions

scd_type    = int(scd_type)
rows_copied = int(rows_copied)
load_action = load_action.strip().upper()
assert load_action in ("FULL", "INCREMENTAL"), f"Unsupported load_action {load_action}"
assert scd_type in (1, 2), f"Unsupported scd_type {scd_type}"


def q(c: str) -> str:
    """Backtick-quote an identifier for Spark SQL."""
    return "`" + c.replace("`", "``") + "`"


def norm_lsn(col):
    """'0x' + 20 upper-case hex chars, so string comparison == LSN order."""
    return F.concat(F.lit("0x"), F.upper(F.substring(col, 3, 20)))


def read_landing(path):
    try:
        df = spark.read.parquet(path)
    except AnalysisException as e:
        if "PATH_NOT_FOUND" in str(e) or "Path does not exist" in str(e):
            return None
        raise
    # drop raw CDC system columns (__$start_lsn, __$operation, __$update_mask, __$seqval ...)
    df = df.drop(*[c for c in df.columns if c.startswith("__$")])
    missing = [c for c in CDC_META if c not in df.columns]
    assert not missing, f"Landing data is missing CDC columns {missing} - check the Copy source query"
    return (df.withColumn("cdc_lsn", norm_lsn(F.col("cdc_lsn")))
              .withColumn("cdc_operation", F.col("cdc_operation").cast("int"))
              .withColumn("cdc_commit_time", F.col("cdc_commit_time").cast("timestamp")))


def business_cols(schema_cols):
    return [c for c in schema_cols if c.lower() not in SYSTEM_COLS]


def resolve_keys(cols):
    by_lower = {c.lower(): c for c in cols}
    raw = [k.strip() for k in key_columns.split(",") if k.strip()]
    assert raw, "key_columns is empty"
    bad = [k for k in raw if k.lower() not in by_lower]
    assert not bad, f"Key column(s) {bad} not found in data columns {cols}"
    return [by_lower[k.lower()] for k in raw]


def latest_per_key(df, keys):
    """Safety net: keep only the last change per key (net changes should already be 1 row/key)."""
    w = Window.partitionBy(*[F.col(q(k)) for k in keys]).orderBy(F.col("cdc_lsn").desc())
    return df.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1").drop("__rn")


def ensure_schema_exists(table):
    if "." in table:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {table.rsplit('.', 1)[0]}")


def evolve_schema(table, df):
    """Add source columns that the target doesn't have yet (new capture instance, new column)."""
    existing = {f.name.lower() for f in spark.table(table).schema.fields}
    new = [f for f in df.schema.fields
           if f.name.lower() not in existing and f.name.lower() not in SYSTEM_COLS]
    if new:
        ddl = ", ".join(f"{q(f.name)} {f.dataType.simpleString()}" for f in new)
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({ddl})")
    return [f.name for f in new]


def last_op_metrics(table):
    row = DeltaTable.forName(spark, table).history(1).select("operation", "operationMetrics").first()
    return {"operation": row["operation"], **(row["operationMetrics"] or {})}


def with_audit(df):
    return (df.withColumn("_cdc_lsn", F.col("cdc_lsn"))
              .withColumn("_loaded_at", F.current_timestamp())
              .withColumn("_run_id", F.lit(run_id)))

# %%
# ---------------------------------------------------------------------
# SCD Type 1
# ---------------------------------------------------------------------
def scd1_full(df, keys):
    biz = business_cols(df.columns)
    out = with_audit(df).select(*[F.col(q(c)) for c in biz + AUDIT_COLS])
    (out.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(target_table))
    return {"mode": "scd1_overwrite", **last_op_metrics(target_table)}


def scd1_merge(df, keys):
    biz = business_cols(df.columns)
    src = with_audit(latest_per_key(df, keys))
    src.createOrReplaceTempView(SRC_VIEW)

    cols  = biz + AUDIT_COLS
    on    = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in keys)
    upd   = ", ".join(f"t.{q(c)} = s.{q(c)}" for c in cols)
    icols = ", ".join(q(c) for c in cols)
    ivals = ", ".join(f"s.{q(c)}" for c in cols)
    # LSN guard makes re-runs of the same window harmless and blocks stale overwrites
    guard = "(t._cdc_lsn IS NULL OR s.cdc_lsn >= t._cdc_lsn)"

    spark.sql(f"""
        MERGE INTO {target_table} t
        USING {SRC_VIEW} s
        ON {on}
        WHEN MATCHED AND s.cdc_operation = 1  AND {guard} THEN DELETE
        WHEN MATCHED AND s.cdc_operation <> 1 AND {guard} THEN UPDATE SET {upd}
        WHEN NOT MATCHED AND s.cdc_operation <> 1 THEN INSERT ({icols}) VALUES ({ivals})
    """)
    return {"mode": "scd1_merge", **last_op_metrics(target_table)}

# %%
# ---------------------------------------------------------------------
# SCD Type 2
#   _valid_from / _valid_to from CDC commit time, _is_current flag,
#   _is_deleted set on the version closed by a source delete.
#   Two steps (close, then insert), both idempotent on re-run via _cdc_lsn.
# ---------------------------------------------------------------------
def scd2_rows(df):
    biz = business_cols(df.columns)
    return (with_audit(df)
            .select(*[F.col(q(c)) for c in biz],
                    F.col("cdc_commit_time").alias("_valid_from"),
                    F.lit(None).cast("timestamp").alias("_valid_to"),
                    F.lit(True).alias("_is_current"),
                    F.lit(False).alias("_is_deleted"),
                    *[F.col(c) for c in AUDIT_COLS]))


def scd2_create(df, keys):
    (scd2_rows(df.filter("cdc_operation <> 1"))
        .write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(target_table))
    return {"mode": "scd2_initial", **last_op_metrics(target_table)}


def scd2_apply(changes, keys):
    changes = latest_per_key(changes, keys).withColumn("_run_id", F.lit(run_id)).cache()
    changes.createOrReplaceTempView(SRC_VIEW)
    on = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in keys)

    # 1) close the current version for every changed / deleted key
    spark.sql(f"""
        MERGE INTO {target_table} t
        USING {SRC_VIEW} s
        ON {on} AND t._is_current = true
        WHEN MATCHED AND (t._cdc_lsn IS NULL OR t._cdc_lsn < s.cdc_lsn) THEN UPDATE SET
            t._valid_to   = s.cdc_commit_time,
            t._is_current = false,
            t._is_deleted = (s.cdc_operation = 1),
            t._run_id     = s._run_id
    """)
    close_metrics = last_op_metrics(target_table)

    # 2) insert new current versions (skip any already written by a previous attempt)
    already = (spark.table(target_table)
               .select(*[F.col(q(k)) for k in keys], F.col("_cdc_lsn").alias("cdc_lsn")))
    to_insert = (changes.filter("cdc_operation <> 1")
                        .join(already, keys + ["cdc_lsn"], "left_anti"))
    n_insert = to_insert.count()
    if n_insert:
        scd2_rows(to_insert.drop("_run_id")).write.format("delta").mode("append").saveAsTable(target_table)
    changes.unpersist()
    return {"mode": "scd2_merge",
            "versions_closed": close_metrics.get("numTargetRowsUpdated"),
            "versions_inserted": n_insert}


def scd2_reload(snap, keys):
    """
    Snapshot reload on an existing SCD2 table (initial LSN purged / forced reset).
    History is preserved: the snapshot is diffed against current rows and turned
    into synthetic changes (upsert for new/changed keys, delete for missing keys).
    """
    cur = spark.table(target_table).filter("_is_current = true")
    tgt_biz = business_cols(cur.columns)

    batch_lsn = to_lsn_hex.upper().replace("0X", "0x") if to_lsn_hex else None
    assert batch_lsn, "to_lsn_hex parameter is required for an SCD2 reload"

    if snap is None:                                   # source table is empty
        upserts = None
        snap_keys = None
        batch_ts = F.current_timestamp()
    else:
        cmp_cols = [c for c in business_cols(snap.columns) if c not in keys and c in tgt_biz]

        def row_hash(df):
            return F.sha2(F.concat_ws("\u0001", *[F.coalesce(F.col(q(c)).cast("string"), F.lit("\u0000"))
                                                   for c in cmp_cols]), 256)

        cur_h = cur.select(*[F.col(q(k)) for k in keys], row_hash(cur).alias("__h_cur"))
        upserts = (snap.withColumn("__h", row_hash(snap))
                       .join(cur_h, keys, "left")
                       .filter("__h_cur IS NULL OR __h <> __h_cur")
                       .drop("__h", "__h_cur"))
        snap_keys = snap.select(*[F.col(q(k)) for k in keys])
        batch_ts = F.lit(snap.select("cdc_commit_time").first()[0]).cast("timestamp")

    gone = cur if snap_keys is None else cur.join(snap_keys, keys, "left_anti")
    deletes = (gone.select(*[F.col(q(c)) for c in tgt_biz])
                   .withColumn("cdc_lsn", F.lit(batch_lsn))
                   .withColumn("cdc_operation", F.lit(1))
                   .withColumn("cdc_commit_time", batch_ts))

    changes = deletes if upserts is None else upserts.unionByName(deletes, allowMissingColumns=True)
    result = scd2_apply(changes, keys)
    result["mode"] = "scd2_reload_diff"
    return result

# %%
# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
result = {"target_table": target_table, "load_action": load_action, "scd_type": scd_type}

df = read_landing(landing_path)
if df is not None and df.limit(1).count() == 0:
    df = None

if df is None and not (load_action == "FULL" and rows_copied == 0):
    if load_action == "INCREMENTAL":
        result["status"] = "no_rows"
    else:
        raise RuntimeError(f"FULL load but no data at {landing_path} and rows_copied={rows_copied}. "
                           "Refusing to treat a missing file as an empty source.")
else:
    exists = spark.catalog.tableExists(target_table)
    ensure_schema_exists(target_table)

    if df is not None:
        keys = resolve_keys(df.columns)
        result["rows_in"] = df.count()
        if exists:
            result["columns_added"] = evolve_schema(target_table, df)
    elif exists:
        keys = resolve_keys(spark.table(target_table).columns)
    else:
        keys = None

    if load_action == "INCREMENTAL":
        if not exists:
            raise RuntimeError(f"{target_table} does not exist but load_action=INCREMENTAL. "
                               "Run etl.usp_cdc_reset for this table to force a FULL load.")
        result.update(scd1_merge(df, keys) if scd_type == 1 else scd2_apply(df, keys))

    else:  # FULL
        if df is None and not exists:
            result["status"] = "source_empty_table_not_created"
        elif df is None and scd_type == 1:
            spark.sql(f"DELETE FROM {target_table}")
            result.update({"mode": "scd1_truncate", **last_op_metrics(target_table)})
        elif scd_type == 1:
            result.update(scd1_full(df, keys))
        elif not exists:
            result.update(scd2_create(df, keys))
        else:
            result.update(scd2_reload(df, keys))

    result.setdefault("status", "applied")

print(json.dumps(result, default=str, indent=2))

# %%
notebookutils.notebook.exit(json.dumps(result, default=str))
