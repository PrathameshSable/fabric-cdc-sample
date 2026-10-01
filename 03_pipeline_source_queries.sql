/* =====================================================================
   03 - PIPELINE SOURCE QUERIES  (pl_cdc_table)
   Paste each block into the activity's Query box as *dynamic content*.
   @{...} tokens are Fabric pipeline string interpolation.
   Author: Prathamesh Sable
   ===================================================================== */


/* ---------------------------------------------------------------------
   A) Lookup_LSN_Window     connection: SQL Server (gateway)   First row only: ON
      Decides the load action and the LSN window for this table.

      load_action:
        FULL                - no watermark yet, or watermark older than the
                              capture instance's min LSN (changes purged)
        INCREMENTAL         - changes exist for THIS table in (from, to]
        NOCHANGE            - nothing for this table; watermark still moves
                              to the DB max LSN so it never ages out
        NO_CAPTURE_INSTANCE - misconfiguration -> fail the run
   --------------------------------------------------------------------- */
SET NOCOUNT ON;

DECLARE @ci    sysname    = N'@{pipeline().parameters.capture_instance}';
DECLARE @last  binary(10) = TRY_CONVERT(binary(10), NULLIF('@{pipeline().parameters.last_lsn_hex}', ''), 1);

-- read the DB max LSN FIRST; everything below is bounded by it
DECLARE @max   binary(10) = sys.fn_cdc_get_max_lsn();
DECLARE @min   binary(10) = sys.fn_cdc_get_min_lsn(@ci);
DECLARE @from  binary(10) = CASE WHEN @last IS NULL THEN NULL ELSE sys.fn_cdc_increment_lsn(@last) END;
DECLARE @tmax  binary(10) = NULL;
DECLARE @action varchar(20), @reason varchar(200);

IF @min IS NULL OR @min = 0x00000000000000000000
    SELECT @action = 'NO_CAPTURE_INSTANCE', @reason = 'Capture instance not found: ' + @ci;
ELSE IF @last IS NULL
    SELECT @action = 'FULL', @reason = 'Initial load (no watermark)';
ELSE IF @from < @min
    SELECT @action = 'FULL', @reason = 'Watermark older than capture instance min LSN (changes purged or instance recreated)';
ELSE IF @from > @max
    SELECT @action = 'NOCHANGE', @reason = 'No new LSNs in database';
ELSE
BEGIN
    DECLARE @sql nvarchar(max) =
        N'SELECT @tm = MAX(__$start_lsn) FROM cdc.' + QUOTENAME(@ci + N'_CT') +
        N' WHERE __$start_lsn >= @f AND __$start_lsn <= @t;';
    EXEC sys.sp_executesql @sql,
         N'@f binary(10), @t binary(10), @tm binary(10) OUTPUT',
         @f = @from, @t = @max, @tm = @tmax OUTPUT;

    SELECT @action = CASE WHEN @tmax IS NULL THEN 'NOCHANGE' ELSE 'INCREMENTAL' END,
           @reason = CASE WHEN @tmax IS NULL THEN 'No changes for this table in window' ELSE 'Changes found' END;
END;

SELECT  @action                                  AS load_action,
        @reason                                  AS reason,
        CONVERT(varchar(22), @from, 1)           AS from_lsn_hex,
        CONVERT(varchar(22),
            CASE WHEN @action = 'NOCHANGE' AND @last IS NOT NULL AND @last > @max
                 THEN @last ELSE @max END, 1)    AS to_lsn_hex,
        CONVERT(varchar(22), @min, 1)            AS min_lsn_hex;


/* ---------------------------------------------------------------------
   B) Copy_Incremental      source: SQL Server (gateway)
      sink: Lakehouse > Files > Parquet
      Net changes, one row per key. 'all with merge' => __$operation
      1 = delete, 5 = insert/update.
   --------------------------------------------------------------------- */
SELECT  CONVERT(varchar(22), c.__$start_lsn, 1)   AS cdc_lsn,
        CAST(c.__$operation AS int)               AS cdc_operation,
        CAST(m.tran_end_time AS datetime2(3))     AS cdc_commit_time,
        c.*
FROM    cdc.fn_cdc_get_net_changes_@{pipeline().parameters.capture_instance}(
            CONVERT(binary(10), '@{activity('Lookup_LSN_Window').output.firstRow.from_lsn_hex}', 1),
            CONVERT(binary(10), '@{activity('Lookup_LSN_Window').output.firstRow.to_lsn_hex}', 1),
            N'all with merge') AS c
LEFT JOIN cdc.lsn_time_mapping AS m
       ON m.start_lsn = c.__$start_lsn;


/* ---------------------------------------------------------------------
   C) Copy_Full             source: SQL Server (gateway)
      sink: Lakehouse > Files > Parquet
      Snapshot stamped with to_lsn (captured BEFORE the copy starts).
      Changes committed while the snapshot runs are re-applied by the
      next incremental; the notebook's MERGE is idempotent, so that's safe.
      Set Isolation level = Snapshot on the source if the DB allows it.
   --------------------------------------------------------------------- */
SELECT  '@{activity('Lookup_LSN_Window').output.firstRow.to_lsn_hex}'   AS cdc_lsn,
        CAST(5 AS int)                                                    AS cdc_operation,
        CAST(sys.fn_cdc_map_lsn_to_time(
             CONVERT(binary(10), '@{activity('Lookup_LSN_Window').output.firstRow.to_lsn_hex}', 1))
             AS datetime2(3))                                             AS cdc_commit_time,
        t.*
FROM    [@{pipeline().parameters.source_schema}].[@{pipeline().parameters.source_table}] AS t;
