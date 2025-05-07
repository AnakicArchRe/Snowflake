CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.RESOLVE_SCENARIO_RETROCONTRACTS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    create or replace table economic_model_computed.retrocontract_scenario as
        with scenarioRetroContracts as (
            select 
                distinct scenarioid, retrocontractid
            from 
                economic_model_scenario.scenario s
                cross join table(split_to_table(coalesce(s.analysis_retrocontractids, ''), ',')) t
                inner join economic_model_revoext.retrocontract r on trim(t.value) = r.retrocontractid
            where 
                s.isactive = 1
        )
        , witOrigAndResolvedValues as (
            select distinct
                sp.scenarioid, 
                rc.* exclude (commissiononnetpremium, level, isactive, profitcommissionpctofprofit, ReinsuranceBrokerageOnNetPremium, ReinsuranceExpensesOnCededCapital, ReinsuranceExpensesOnCededPremium, netcessionlockin),
                coalesce(last_value(rc_o.isactive) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.isactive) isactive,
                coalesce(last_value(rc_o.level) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.level) level,
                coalesce(last_value(rc_o.commission) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.commissiononnetpremium) commissiononnetpremium,
                coalesce(last_value(rc_o.profitcommission) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.profitcommissionpctofprofit) profitcommissionpctofprofit,
                coalesce(last_value(rc_o.ReinsuranceBrokerageOnNetPremium) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.ReinsuranceBrokerageOnNetPremium) ReinsuranceBrokerageOnNetPremium,
                coalesce(last_value(rc_o.ReinsuranceExpensesOnCededCapital) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.ReinsuranceExpensesOnCededCapital) ReinsuranceExpensesOnCededCapital,
                coalesce(last_value(rc_o.ReinsuranceExpensesOnCededPremium) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.ReinsuranceExpensesOnCededPremium) ReinsuranceExpensesOnCededPremium,
                coalesce(last_value(rc_o.netcessionlockin) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.netcessionlockin, false) netcessionlockin,
                last_value(case when irc.retrocontractid is null then false else true end) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc) includeInAnalysis,
                last_value(case when rc_o.retrocontractid is null then false else true end) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc) HasOverride,
                last_value(rc_o.targetcollateralcalculated) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc) TargetCollateralCalculated,

                rc.isactive as isactive_original,
                rc.level as level_original,
                rc.commissiononnetpremium as commissiononnetpremium_original,
                rc.profitcommissionpctofprofit as profitcommissionpctofprofit_original,
                rc.ReinsuranceBrokerageOnNetPremium as ReinsuranceBrokerageOnNetPremium_original,
                rc.ReinsuranceExpensesOnCededCapital as ReinsuranceExpensesOnCededCapital_original,
                rc.ReinsuranceExpensesOnCededPremium as ReinsuranceExpensesOnCededPremium_original,
            from
                economic_model_revoext.retrocontract rc
                cross join economic_model_scenario.scenario_parts sp
                left outer join economic_model_scenario.retrocontract_override rc_o on sp.partid = rc_o.scenarioid and rc.retrocontractid = rc_o.retrocontractid
                left outer join scenarioRetroContracts irc on irc.retrocontractid = rc.retrocontractid and irc.scenarioid = sp.partid
                -- make sure the retro interacts with portfolios visible in the scenario
                inner join economic_model_computed.cessionflag f on rc.retrocontractid = f.retrocontractid and sp.partid = f.scenarioid
        )
        select
            * exclude (isactive_original, level_original, commissiononnetpremium_original, profitcommissionpctofprofit_original, ReinsuranceBrokerageOnNetPremium_original, ReinsuranceExpensesOnCededCapital_original, ReinsuranceExpensesOnCededPremium_original),
            economic_model_computed.concat_non_null(
                economic_model_computed.compare_and_note(isactive, isactive_original, 'IsActive'),
                economic_model_computed.compare_and_note(level, level_original, 'Level'),
                economic_model_computed.compare_and_note(commissiononnetpremium, commissiononnetpremium_original, 'CommissionOnNetPremium'),
                economic_model_computed.compare_and_note(profitcommissionpctofprofit, profitcommissionpctofprofit_original, 'ProfitCommissionPctOfProfit'),
                economic_model_computed.compare_and_note(ReinsuranceBrokerageOnNetPremium, ReinsuranceBrokerageOnNetPremium_original, 'ReinsuranceBrokerageOnNetPremium'),
                economic_model_computed.compare_and_note(ReinsuranceExpensesOnCededCapital, ReinsuranceExpensesOnCededCapital_original, 'ReinsuranceExpensesOnCededCapital'),
                economic_model_computed.compare_and_note(ReinsuranceExpensesOnCededPremium, ReinsuranceExpensesOnCededPremium_original, 'ReinsuranceExpensesOnCededPremium')
            ) AS notes
        from 
            witOrigAndResolvedValues
        ;
END
;