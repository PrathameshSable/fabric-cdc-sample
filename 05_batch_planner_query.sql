/* =====================================================================
   05 - BATCH PLANNER  (pl_cdc_batch > Lookup_Plan, source: SQL Server)
   ONE round-trip to the source for the whole load group, instead of one
   Lookup per table. Returns one row per table with:
     load_action  FULL | INCREMENTAL | NOCHANGE | NO_CAPTURE_INSTANCE
     from/to LSN, status, landing_path, and the ready-made copy_query.
   Lookup setting: First row only = OFF  (limit 5,000 rows / 4 MB).
   Requires source DB compatibility level >= 130 (OPENJSON).
   Paste as dynamic content.
   Author: Prathamesh Sable
   ===================================================================== */
SET NOCOUNT ON;

DECLARE @ctrl   nvarchar(max) = N'@{replace(activity('Lookup_Control').output.firstRow.control_json, '''', '''''')}';
DECLARE @run_id varchar(64)   = '@{pipeline().RunId}';
DECLARE @zero   binary(10)    = 0x00000000000000000000;
DECLARE @max    binary(10)    = sys.fn_cdc_get_max_lsn();      -- read FIRST, bounds everything

CREATE TABLE #plan
(
    control_id       int           NOT NULL PRIMARY KEY,
    source_system    varchar(50)   NOT NULL,
    source_schema    sysname       NOT NULL,
    source_table     sysname       NOT NULL,
    capture_instance sysname       NOT NULL,
    key_columns      nvarchar(500) NOT NULL,
    target_table     nvarchar(256) NOT NULL,
    scd_type         tinyint       NOT NULL,
    last_lsn         binary(10)    NULL,
    min_lsn          binary(10)    NULL,
    from_lsn         binary(10)    NULL,
    tbl_max          binary(10)    NULL,
    load_action      varchar(30)   NULL,
    reason           varchar(300)  NULL
);

INSERT #plan (control_id, source_system, source_schema, source_table, capture_instance,
              key_columns, target_table, scd_type, last_lsn, min_lsn)
SELECT j.control_id, j.source_system, j.source_schema, j.source_table, j.capture_instance,
       j.key_columns, j.target_table, j.scd_type,
       TRY_CONVERT(binary(10), NULLIF(j.last_lsn_hex, ''), 1),
       ISNULL(sys.fn_cdc_get_min_lsn(j.capture_instance), @zero)
FROM OPENJSON(@ctrl) WITH (
        control_id       int,
        source_system    varchar(50),
        source_schema    sysname,
        source_table     sysname,
        capture_instance sysname,
        key_columns      nvarchar(500),
        target_table     nvarchar(256),
        scd_type         tinyint,
        last_lsn_hex     varchar(22)) AS j;

UPDATE #plan SET from_lsn = sys.fn_cdc_increment_lsn(last_lsn) WHERE last_lsn IS NOT NULL;

/* Probe each candidate's change table (clustered on __$start_lsn => cheap seek) */
DECLARE @id int, @ci sysname, @f binary(10), @tm binary(10), @sql nvarchar(max);
DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT control_id, capture_instance, from_lsn
    FROM   #plan
    WHERE  min_lsn <> @zero AND from_lsn IS NOT NULL AND from_lsn >= min_lsn AND from_lsn <= @max;
OPEN cur;
FETCH NEXT FROM cur INTO @id, @ci, @f;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @tm = NULL;
    SET @sql = N'SELECT @tm = MAX(__$start_lsn) FROM cdc.' + QUOTENAME(@ci + N'_CT') +
               N' WHERE __$start_lsn >= @f AND __$start_lsn <= @t;';
    EXEC sys.sp_executesql @sql, N'@f binary(10), @t binary(10), @tm binary(10) OUTPUT',
         @f = @f, @t = @max, @tm = @tm OUTPUT;
    UPDATE #plan SET tbl_max = @tm WHERE control_id = @id;
    FETCH NEXT FROM cur INTO @id, @ci, @f;
END;
CLOSE cur; DEALLOCATE cur;

UPDATE #plan SET
    load_action = CASE
        WHEN min_lsn = @zero       THEN 'NO_CAPTURE_INSTANCE'
        WHEN last_lsn IS NULL      THEN 'FULL'
        WHEN from_lsn < min_lsn    THEN 'FULL'
        WHEN from_lsn > @max       THEN 'NOCHANGE'
        WHEN tbl_max IS NULL       THEN 'NOCHANGE'
        ELSE 'INCREMENTAL' END,
    reason = CASE
        WHEN min_lsn = @zero       THEN 'Capture instance not found: ' + capture_instance
        WHEN last_lsn IS NULL      THEN 'Initial load (no watermark)'
        WHEN from_lsn < min_lsn    THEN 'Watermark older than capture instance min LSN (purged / new instance)'
        WHEN from_lsn > @max       THEN 'No new LSNs in database'
        WHEN tbl_max IS NULL       THEN 'No changes for this table in window'
        ELSE 'Changes found' END;

;WITH p AS (
    SELECT *,
           CONVERT(varchar(22), from_lsn, 1) AS from_hex,
           CONVERT(varchar(22), CASE WHEN load_action = 'NOCHANGE' AND last_lsn > @max
                                     THEN last_lsn ELSE @max END, 1) AS to_hex
    FROM #plan
)
SELECT
    control_id, source_system, source_schema, source_table, capture_instance,
    key_columns, target_table, scd_type, load_action, reason,
    ISNULL(from_hex, '')                                         AS from_lsn_hex,
    to_hex                                                       AS to_lsn_hex,
    CASE load_action WHEN 'NOCHANGE'            THEN 'Succeeded'
                     WHEN 'NO_CAPTURE_INSTANCE' THEN 'Failed'
                     ELSE 'Pending' END                          AS status,
    CASE WHEN load_action = 'NO_CAPTURE_INSTANCE' THEN reason END AS error_message,
    CONCAT('cdc_landing/', source_system, '/', capture_instance, '/', @run_id) AS landing_path,
    CASE load_action
      WHEN 'FULL' THEN CONCAT(
        N'SELECT ''', to_hex, N''' AS cdc_lsn, CAST(5 AS int) AS cdc_operation, ',
        N'CAST(sys.fn_cdc_map_lsn_to_time(', to_hex, N') AS datetime2(3)) AS cdc_commit_time, t.* ',
        N'FROM ', QUOTENAME(source_schema), N'.', QUOTENAME(source_table), N' AS t;')
      WHEN 'INCREMENTAL' THEN CONCAT(
        N'SELECT CONVERT(varchar(22), c.__$start_lsn, 1) AS cdc_lsn, CAST(c.__$operation AS int) AS cdc_operation, ',
        N'CAST(m.tran_end_time AS datetime2(3)) AS cdc_commit_time, c.* ',
        N'FROM cdc.', QUOTENAME(N'fn_cdc_get_net_changes_' + capture_instance),
        N'(', from_hex, N', ', to_hex, N', N''all with merge'') AS c ',
        N'LEFT JOIN cdc.lsn_time_mapping AS m ON m.start_lsn = c.__$start_lsn;')
    END                                                          AS copy_query
FROM p
ORDER BY CASE load_action WHEN 'FULL' THEN 0 WHEN 'INCREMENTAL' THEN 1 ELSE 2 END, control_id;

DROP TABLE #plan;
