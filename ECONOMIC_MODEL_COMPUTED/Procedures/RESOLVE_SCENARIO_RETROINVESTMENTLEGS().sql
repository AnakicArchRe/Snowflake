CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.RESOLVE_SCENARIO_RETROINVESTMENTLEGS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    create or replace table economic_model_computed.retroinvestmentleg_scenario as
        with
            withResolvedAndOriginalValues as (
                select distinct
                        sp.scenarioid, 
                        l.* exclude (investmentsigned, investmentsignedamt),
                        coalesce(last_value(l_o.INVESTMENTSIGNEDPCT) ignore nulls over (partition by sp.scenarioid, l.retroinvestmentlegid order by depth asc), l.investmentsigned) investmentsigned,
                        coalesce(last_value(l_o.INVESTMENTSIGNEDAMT) ignore nulls over (partition by sp.scenarioid, l.retroinvestmentlegid order by depth asc), l.investmentsignedamt) investmentsignedamt,
                        last_value(l_o.investmentcalculatedpct) ignore nulls over (partition by sp.scenarioid, l.retroinvestmentlegid order by depth asc) investmentcalculatedpct,
                        l.investmentsigned as investmentsigned_original,
                        l.investmentsignedamt as investmentsignedamt_original
                    from
                        economic_model_staging.retroinvestmentleg l
                        cross join economic_model_scenario.scenario_parts sp
                        left outer join economic_model_scenario.retroinvestmentleg_override l_o on sp.partid = l_o.scenarioid and l.retroinvestmentlegid = l_o.retroinvestmentlegid
                        -- make sure the retro interacts with portfolios visible in the scenario
                        inner join economic_model_staging.retroconfiguration rc on l.retroconfigurationid = rc.retroconfigurationid
                        inner join economic_model_computed.cessionflag f on rc.retrocontractid = f.retrocontractid and sp.partid = f.scenarioid
            )
            select 
                * exclude (investmentsigned_original, investmentsignedamt_original),
                economic_model_computed.concat_non_null(
                    economic_model_computed.compare_and_note(investmentsigned, investmentsigned_original, 'InvestmentSigned'),
                    economic_model_computed.compare_and_note(investmentsignedamt, investmentsignedamt_original, 'InvestmentsignedAmt')
                ) AS notes
            from 
                withResolvedAndOriginalValues
            ;

END
;