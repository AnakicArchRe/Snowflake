CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_MANAGEMENT.CREATESNAPSHOT(SNAPSHOTID VARCHAR, DESCRIPTION VARCHAR, CREATEDBY VARCHAR, OVERWRITE BOOLEAN)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS
DECLARE
    TABLE_NAME varchar;
    TABLE_SCHEMA varchar;
    x varchar;
    ddl varchar;
    TBL_CURR CURSOR FOR 
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE 
            TABLE_CATALOG = 'ACTUARIAL_ILS_POC' 
            AND TABLE_SCHEMA in ('ECONOMIC_MODEL_RAW', 'ECONOMIC_MODEL_REVOEXT', 'ECONOMIC_MODEL_SCENARIO', 'ECONOMIC_MODEL_MANAGEMENT')
            AND TABLE_TYPE = 'BASE TABLE';
BEGIN

    -- begin transaction;

    -- todo: make schema names dynamic?
    set ddl:= (select 
        concat(
                get_ddl('schema', 'economic_model_raw') || '\n',
                get_ddl('schema', 'economic_model_revoext') || '\n',
                get_ddl('schema', 'economic_model_scenario') || '\n',
                get_ddl('schema', 'economic_model_staging') || '\n',
                get_ddl('schema', 'economic_model_computed') || '\n',
                get_ddl('schema', 'economic_model_management')
        )
    );

    // delete previous records with this snapshotid
    delete from economic_model_management.SnapshotInfo
    where snapshotid = :snapshotid;

    // delete previous table records with this snapshotid
    delete from economic_model_management.TableSnapshotInfo
    where snapshotid = :snapshotid;

    // insert record for this snapshotid
    insert into economic_model_management.SnapshotInfo(snapshotid, description, createdby, createdon, ddl)
    values (:snapshotid, :description, :createdby, current_timestamp(), :ddl);


    -- make sure to add "execute as caller" after "language sql" in the procedure
    -- definition. My powershell cmdlets keeps stripping it out as it can't read the "execute as" parameter from metadata (todo: look into options for this)
    -- // remove any old data from previous snapshot with the same id
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