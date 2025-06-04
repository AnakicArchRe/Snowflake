CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.POPULATE_RETROPROGRAMS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    -- work out whether a layer cedes to a retroprogram (not contract)...
    create or replace table economic_model_staging.RetroProgram as
        select 
            concat(source_db, '_', RetroProgramId) RetroProgramId,
            source_db as Source, 
            Name, 
            Inception, 
            Expiration, 
            RetroProgramType, 
            RetroLevelType + 1 as Level,
            0 AS ReinsuranceBrokerageOnNetPremium,
            OVERRIDE AS CommissionOnNetPremium,
            PROFITCOMM AS ProfitCommission,
            CASE WHEN RHOE  > 0 THEN 0 ELSE HURDLERATE END AS ReinsuranceExpensesOnCededCapital,
            RHOE AS ReinsuranceExpensesOnCededPremium,
            case when (cedeselectiontype = 1 and retroleveltype = 0) then 1 else 0 end as IsSpecific,
            status,
            case when status in (22,10) then true else false end as IsActive
        from 
            economic_model_raw.retroprogram
        where 
            isactive = 1 
            and isdeleted = 0 
            -- these ones are either active or can be made active
            and status in (22, 10, 25, 1);

end
;