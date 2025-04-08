CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.POPULATE_PORTFOLIOS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
$$
BEGIN

    -- portfolio
    create or replace table ECONOMIC_MODEL_STAGING.Portfolio as   
        select 
            concat(source_db, '_', p.PortfolioId) as PortfolioId, 
            source_db as Source, 
            Name, 
            UWYear
        from 
            economic_model_raw.portfolio p
        where 
            isactive = 1 and isdeleted = 0;

end
$$;