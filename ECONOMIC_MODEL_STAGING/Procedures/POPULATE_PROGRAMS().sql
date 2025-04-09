CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.POPULATE_PROGRAMS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin
    
    -- program
    create or replace TABLE economic_model_staging.PROGRAM as
        select 
            concat(p.source_db, '_', ProgramId) as ProgramId, 
            concat(p.source_db, '_',p.CedentId) as CedentId, 
            c.Name as CedentName,
            cg.Name as CedentGroup,
            cp.LEGALENTCODE as Company
        from 
            economic_model_raw.program p
            inner join economic_model_raw.cedent c on p.cedentid = c.cedentid and c.source_db = p.source_db
            inner join economic_model_raw.cedentgroup cg on c.cedentgroupid = cg.cedentgroupid and c.source_db = cg.source_db
            inner join economic_model_raw.company cp on p.companyid = cp.companyid and p.source_db = cp.source_db;
end
;