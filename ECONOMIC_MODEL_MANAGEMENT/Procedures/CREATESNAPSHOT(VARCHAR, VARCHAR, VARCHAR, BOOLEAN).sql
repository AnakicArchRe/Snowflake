CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_MANAGEMENT.CREATESNAPSHOT(SNAPSHOTID VARCHAR, DESCRIPTION VARCHAR, CREATEDBY VARCHAR, OVERWRITE BOOLEAN)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS
DECLARE
    TABLE_NAME varchar;
    TABLE_SCHEMA varchar;
    x varchar;
    TBL_CURR CURSOR FOR 
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_CATALOG = 'ACTUARIAL_ILS_POC' AND TABLE_SCHEMA = 'ECONOMIC_MODEL_RAW' AND TABLE_TYPE = 'BASE TABLE';
BEGIN

    begin transaction;

    // delete previous records with this snapshotid
    delete from economic_model_management.SnapshotInfo
    where snapshotid = :snapshotid;

    // delete previous table records with this snapshotid
    delete from economic_model_management.TableSnapshotInfo
    where snapshotid = :snapshotid;

    // insert record for this snapshotid
    insert into economic_model_management.SnapshotInfo(snapshotid, description, createdby, createdon)
    values (:snapshotid, :description, :createdby, current_timestamp());

    // remove any old data from previous snapshot with the same id
    execute immediate 
        'REMOVE @economic_model_management.snapshots/' || :snapshotid || '/';

    -- Get all table names from the source schema
    FOR record IN TBL_CURR DO
        set TABLE_NAME := record.TABLE_NAME;
        set TABLE_SCHEMA := record.TABLE_SCHEMA;

        insert into economic_model_management.TableSnapshotInfo(snapshotid, tableSchema, tableName)
        values(:snapshotid, :table_schema, :table_name);
        
        // store data for this snapshot id
        execute immediate 
            'COPY INTO @economic_model_management.snapshots/' || :snapshotid || '/' || :table_schema ||  '/' || :table_name || 
            ' FROM (SELECT * FROM ' || :table_schema ||  '.' || :table_name || ')
            FILE_FORMAT = (TYPE = parquet) 
            overwrite=' || :overwrite || '
            header=true';

    END FOR;

    commit;
    
END
;