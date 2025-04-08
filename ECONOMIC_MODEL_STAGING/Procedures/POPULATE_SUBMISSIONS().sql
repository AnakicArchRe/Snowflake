CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.POPULATE_SUBMISSIONS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
$$
begin

    -- submission
    create or replace table economic_model_staging.SUBMISSION as 
        select 
            concat(source_db, '_', SubmissionId) as SubmissionId, 
            source_db as Source, 
            concat(source_db, '_', ProgramId) as ProgramId, 
            Currency, 
            FXDate,
            case 
                when trantype = 1 then 1 
                else -1 
            end as SideSign,
        from 
            economic_model_raw.submission
        where
            isactive = 1 and isdeleted = 0;

end
$$;