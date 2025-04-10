CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.CLEARALL()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
DECLARE
    tbl varchar;
    tblsCurr CURSOR FOR 
        select 
            *,
            concat(table_schema, '.', table_name) as tablefullname
        from 
            information_schema.tables
        where 
        (
            UPPER(table_schema) in ('ECONOMIC_MODEL_RAW', 'ECONOMIC_MODEL_STAGING', 'ECONOMIC_MODEL_COMPUTED')
            -- todo: move these to _computed schema, so we can remove this exception? the scenario override tables
            -- should not be versioned as they are generated from the raw+override data.
            OR (UPPER(TABLE_SCHEMA) = 'ECONOMIC_MODEL_SCENARIO' AND UPPER(TABLE_NAME) LIKE '%_SCENARIO')
        )
        AND table_type LIKE '%TABLE%'
        ;
begin

    FOR tblrecord IN tblsCurr DO
        set tbl := tblrecord.tablefullname;
        truncate identifier(:tbl);
    END FOR;
end
;