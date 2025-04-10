CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_MANAGEMENT.LOADSNAPSHOT(SNAPSHOTID VARCHAR)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS
DECLARE
    TABLE_NAME varchar;
    TABLE_SCHEMA varchar;
    x varchar;
    TBL_CURR CURSOR FOR 
        SELECT *
        FROM economic_model_management.tablesnapshotinfo
        WHERE snapshotid = ?;
BEGIN

    begin transaction;

    open TBL_CURR using (:snapshotid);
    
    -- Get all table names from the source schema
    FOR record IN TBL_CURR DO
        set TABLE_NAME := record.TABLENAME;
        set TABLE_SCHEMA := record.TABLESCHEMA;
       
        -- remove current data
        execute immediate
            'truncate ' || :table_schema || '.' || :table_name;

        -- restore saved data
        execute immediate
            -- restore data from parquet file
            'COPY INTO ' || :table_schema || '.' || :table_name || ' FROM @economic_model_management.snapshots/' || :snapshotid || '/' || :table_schema || '/' || :table_name || '
              FILE_FORMAT = (type=parquet)
              MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;';


    END FOR;
    
    commit;
    
END
;