/* =====================================================================
   06 - CONTROL DB: batch completion  (run in the Fabric SQL database)
   One call closes out many tables: advances watermarks for rows with
   status = 'Succeeded', logs every row. Accepts either a JSON array or
   the batch notebook's exit object ({"results": [...]}).
   Author: Prathamesh Sable
   ===================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_cdc_complete_batch
    @pipeline_run_id varchar(64),
    @results_json    nvarchar(max)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF ISJSON(@results_json) = 0
        THROW 50002, 'usp_cdc_complete_batch: @results_json is not valid JSON.', 1;

    DECLARE @arr nvarchar(max) =
        CASE WHEN LEFT(LTRIM(@results_json), 1) = '{' THEN JSON_QUERY(@results_json, '$.results')
             ELSE @results_json END;

    DECLARE @r TABLE
    (
        control_id    int            NOT NULL PRIMARY KEY,
        load_action   varchar(30)    NULL,
        from_lsn_hex  varchar(22)    NULL,
        to_lsn_hex    varchar(22)    NULL,
        rows_copied   bigint         NULL,
        status        varchar(20)    NULL,
        apply_result  nvarchar(4000) NULL,
        error_message nvarchar(4000) NULL
    );

    INSERT @r
    SELECT control_id, load_action, NULLIF(from_lsn_hex, ''), to_lsn_hex, rows_copied,
           ISNULL(status, 'Failed'), LEFT(apply_result, 4000), LEFT(error_message, 4000)
    FROM OPENJSON(ISNULL(@arr, '[]')) WITH (
            control_id    int,
            load_action   varchar(30),
            from_lsn_hex  varchar(22),
            to_lsn_hex    varchar(22),
            rows_copied   bigint,
            status        varchar(20),
            apply_result  nvarchar(max),
            error_message nvarchar(max));

    BEGIN TRAN;

        UPDATE c
           SET last_lsn_hex     = r.to_lsn_hex,
               last_load_action = r.load_action,
               last_success_utc = SYSUTCDATETIME()
          FROM etl.cdc_control c
          JOIN @r r ON r.control_id = c.control_id
         WHERE r.status = 'Succeeded'
           AND LEN(r.to_lsn_hex) = 22
           AND (   c.last_lsn_hex IS NULL
                OR CONVERT(binary(10), r.to_lsn_hex, 1) >= CONVERT(binary(10), c.last_lsn_hex, 1));

        INSERT etl.cdc_run_log (control_id, pipeline_run_id, load_action, from_lsn_hex, to_lsn_hex,
                                rows_copied, apply_result, status, error_message)
        SELECT control_id, @pipeline_run_id, ISNULL(load_action, 'UNKNOWN'), from_lsn_hex, to_lsn_hex,
               rows_copied, apply_result, status, error_message
        FROM   @r;

    COMMIT;

    SELECT SUM(CASE WHEN status = 'Succeeded' THEN 1 ELSE 0 END) AS succeeded,
           SUM(CASE WHEN status <> 'Succeeded' THEN 1 ELSE 0 END) AS failed
    FROM @r;
END;
GO

/* Onboard in waves: activate the next N inactive tables in a load group */
CREATE OR ALTER PROCEDURE etl.usp_cdc_activate_wave
    @load_group varchar(50) = 'default',
    @wave_size  int         = 25
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE TOP (@wave_size) etl.cdc_control
       SET is_active = 1
     WHERE is_active = 0 AND load_group = @load_group;
    SELECT @@ROWCOUNT AS activated;
END;
GO
