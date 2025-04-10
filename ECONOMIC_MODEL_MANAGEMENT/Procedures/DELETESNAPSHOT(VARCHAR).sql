CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_MANAGEMENT.DELETESNAPSHOT(SNAPSHOTID VARCHAR)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS
BEGIN

    begin transaction;

    // delete previous records with this snapshotid
    delete from economic_model_management.SnapshotInfo
    where snapshotid = :snapshotid;

    // delete previous table records with this snapshotid
    delete from economic_model_management.TableSnapshotInfo
    where snapshotid = :snapshotid;

    // remove any old data from previous snapshot with the same id
    execute immediate 
        'REMOVE @economic_model_management.snapshots/' || :snapshotid || '/';

    commit;
    
END
;