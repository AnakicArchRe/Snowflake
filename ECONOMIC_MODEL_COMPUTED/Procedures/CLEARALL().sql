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