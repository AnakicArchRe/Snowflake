CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.EXPORT_BY_SCENARIO_PARTITIONS(FULL_TABLE_NAME VARCHAR, SCENARIO_ID NUMBER)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS
DECLARE
    curr_scenario_id string;
    sql_cmd  STRING;
    scenariosCurr CURSOR FOR select distinct scenarioid from identifier(?) where coalesce(?, -1) in (-1, scenarioid);
BEGIN


    open scenariosCurr using (FULL_TABLE_NAME, SCENARIO_ID);
    FOR scenario IN scenariosCurr DO
        SET curr_scenario_id := scenario.scenarioid;

        EXECUTE IMMEDIATE 'REMOVE @economic_model_management.export/' || :full_table_name || '/' || curr_scenario_id || '/';

        SET sql_cmd := 
        'COPY INTO @economic_model_management.export/' || full_table_name || '/' || curr_scenario_id || '/' ||
        ' FROM (SELECT * FROM ' || full_table_name || ' WHERE scenarioid = ''' || curr_scenario_id || ''') ' ||
        ' FILE_FORMAT = (TYPE = PARQUET) ';

        EXECUTE IMMEDIATE :sql_cmd;
    END FOR;
END
;