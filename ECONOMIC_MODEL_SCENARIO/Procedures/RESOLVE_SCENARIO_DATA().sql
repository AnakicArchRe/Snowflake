CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_SCENARIO.RESOLVE_SCENARIO_DATA()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    -- todo: Consider if I need the original values inside these tables (those "union" blocks)? 
    -- Is this not just for the scenario editor? If so, I should I should remove that code here 
    -- and put it just in the scenario editor.

    -- scenario parts
    create or replace table economic_model_scenario.scenario_parts as
        with recursive cte as (
            // each scenario consists at least of itself
            select 
                sf.scenarioid,
                sf.scenarioid as partId,
                // use the scenario fxdate for all parts (base+diff)
                sf.fxdate,
                0 as hops
            from 
                economic_model_scenario.scenario sf
            where 
                sf.isactive = 1
            
            union all
        
            // but also of any ancestors it might have
            select 
                sf.scenarioid,
                cte.partId,
                // use the scenario fxdate for all parts (base+diff)
                sf.fxdate,
                cte.hops + 1 as hops
            from 
                economic_model_scenario.scenario sf
                inner join cte on sf.parentscenarioid = cte.scenarioid
            where sf.isactive = 1
        )
        select 
            *, 
            count(*) over (partition by scenarioid) - hops as depth 
        from 
            cte 
        order by 
            scenarioid, partid;

    -- scenario depth (required for processing blocks in order, i.e. parent before child)
    create or replace table economic_model_scenario.scenario_meta as 
        select distinct
            partid as scenarioid, depth
        from 
            economic_model_scenario.scenario_parts;

    -- retrocontract
    create or replace table economic_model_scenario.retrocontract_scenario as
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
                rc.* exclude (commissiononnetpremium, level, isactive, profitcommissionpctofprofit, ReinsuranceBrokerageOnNetPremium, ReinsuranceExpensesOnCededCapital, ReinsuranceExpensesOnCededPremium),
                coalesce(last_value(rc_o.isactive) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.isactive) isactive,
                coalesce(last_value(rc_o.level) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.level) level,
                coalesce(last_value(rc_o.commission) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.commissiononnetpremium) commissiononnetpremium,
                coalesce(last_value(rc_o.profitcommission) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.profitcommissionpctofprofit) profitcommissionpctofprofit,
                coalesce(last_value(rc_o.ReinsuranceBrokerageOnNetPremium) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.ReinsuranceBrokerageOnNetPremium) ReinsuranceBrokerageOnNetPremium,
                coalesce(last_value(rc_o.ReinsuranceExpensesOnCededCapital) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.ReinsuranceExpensesOnCededCapital) ReinsuranceExpensesOnCededCapital,
                coalesce(last_value(rc_o.ReinsuranceExpensesOnCededPremium) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.ReinsuranceExpensesOnCededPremium) ReinsuranceExpensesOnCededPremium,
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
        )
        select
            * exclude (isactive_original, level_original, commissiononnetpremium_original, profitcommissionpctofprofit_original, ReinsuranceBrokerageOnNetPremium_original, ReinsuranceExpensesOnCededCapital_original, ReinsuranceExpensesOnCededPremium_original),
            economic_model_computed.concat_non_null(
                economic_model_scenario.compare_and_note(isactive, isactive_original, 'IsActive'),
                economic_model_scenario.compare_and_note(level, level_original, 'Level'),
                economic_model_scenario.compare_and_note(commissiononnetpremium, commissiononnetpremium_original, 'CommissionOnNetPremium'),
                economic_model_scenario.compare_and_note(profitcommissionpctofprofit, profitcommissionpctofprofit_original, 'ProfitCommissionPctOfProfit'),
                economic_model_scenario.compare_and_note(ReinsuranceBrokerageOnNetPremium, ReinsuranceBrokerageOnNetPremium_original, 'ReinsuranceBrokerageOnNetPremium'),
                economic_model_scenario.compare_and_note(ReinsuranceExpensesOnCededCapital, ReinsuranceExpensesOnCededCapital_original, 'ReinsuranceExpensesOnCededCapital'),
                economic_model_scenario.compare_and_note(ReinsuranceExpensesOnCededPremium, ReinsuranceExpensesOnCededPremium_original, 'ReinsuranceExpensesOnCededPremium')
            ) AS notes
        from 
            witOrigAndResolvedValues
        union
        select 
            null as scenarioid,
            rc.* exclude (commissiononnetpremium, level, isactive, profitcommissionpctofprofit, ReinsuranceBrokerageOnNetPremium, ReinsuranceExpensesOnCededCapital, ReinsuranceExpensesOnCededPremium),
            rc.isactive,
            rc.level,
            rc.commissiononnetpremium commission,
            rc.profitcommissionpctofprofit profitcommission,
            rc.ReinsuranceBrokerageOnNetPremium, 
            rc.ReinsuranceExpensesOnCededCapital, 
            rc.ReinsuranceExpensesOnCededPremium,
            false includeInAnalysis,
            false HasOverride,
            null TargetCollateralCalculated,
            null notes
        from
            economic_model_revoext.retrocontract rc
        ;


    -- unpivoted topupzone overrides
    create or replace table economic_model_scenario.topupzone_override_unpivoted as
        with productGroup as (
            select 
                $1 ProductGroup
            from 
                values 
                    ('LINTEAU'),
                    ('MIDDLE'),
                    ('TOPUP')
        )
        , unpivoted as (
            SELECT 
                topupzoneid, 
                scenarioid, 
                pg.productgroup,
                case pg.productgroup
                    when 'LINTEAU' then linteau_sharefactor
                    when 'MIDDLE' then middle_sharefactor
                    when 'TOPUP' then topup_sharefactor
                    else null
                end as shareFactor,
                case pg.productgroup
                    when 'LINTEAU' then linteau_premiumfactor
                    when 'MIDDLE' then middle_premiumfactor
                    when 'TOPUP' then topup_premiumfactor
                    else null
                end as premiumFactor
            FROM 
                economic_model_scenario.topupzone_override
                cross join productgroup pg
        )
        select 
            * 
        from 
            unpivoted
        where
            shareFactor is not null 
            or premiumFactor is not null
          ;


    -- portlayer
    create or replace table economic_model_scenario.portlayer_scenario as
        with scenarioPortfolios as (
            select 
                scenarioid, portfolioid
            from 
                economic_model_scenario.scenario s
                cross join table(split_to_table(coalesce(s.analysis_PORTFOLIOIDS, ''), ',')) t
                inner join economic_model_staging.portfolio p on trim(t.value) = p.portfolioid
        )
        , withResolvedAndOriginalValues as (
            select distinct
                sp.scenarioid, 
                pl.* exclude (sharefactor, premiumfactor),
                -- find closest (deepest) sharefactor/premiumfactor override, either on the portlayer itself or its topupzone.
                case 
                    -- note: cannot override inforce portlayers
                    -- todo: check other entities and verify this is the case
                    when pl.layerview = 'INFORCE' then pl.sharefactor 
                    else coalesce(last_value(coalesce(pl_o.sharefactor, tz_u_o.sharefactor)) ignore nulls over (partition by sp.scenarioid, pl.portlayerid order by depth asc), pl.sharefactor)
                end sharefactor,
                case 
                    when pl.layerview = 'INFORCE' then pl.premiumfactor 
                    else coalesce(last_value(coalesce(pl_o.premiumfactor, tz_u_o.premiumfactor)) ignore nulls over (partition by sp.scenarioid, pl.portlayerid order by depth asc), pl.premiumfactor) 
                end premiumfactor,
                pl.sharefactor as shareFactor_original,
                pl.premiumfactor as premiumFactor_original
            from
                economic_model_staging.portlayer pl
                cross join economic_model_scenario.scenario_parts sp
                -- limit to portfolios included in scenario (todo: parts and main scenario should have the same list of portfolios)
                inner join scenarioPortfolios pf on pl.portfolioid = pf.portfolioid and pf.scenarioid = sp.scenarioid
                left outer join economic_model_scenario.portlayer_override pl_o on sp.partid = pl_o.scenarioid and pl.portlayerid = pl_o.portlayerid
                left outer join economic_model_scenario.topupzone_override_unpivoted tz_u_o on sp.partid = tz_u_o.scenarioid and pl.topupzoneid = tz_u_o.topupzoneid and tz_u_o.productgroup = pl.productgroup
        )
        select 
            * exclude (shareFactor_original, premiumFactor_original),
            economic_model_computed.concat_non_null(
                economic_model_scenario.compare_and_note(sharefactor, sharefactor_original, 'ShareFactor'),
                economic_model_scenario.compare_and_note(premiumfactor, premiumfactor_original, 'Premiumactor')
            ) AS notes
        from 
            withResolvedAndOriginalValues
        union
        -- include the base values too. This is primarily needed in the scenarioeditor which has to show the parent scenario values or the original values in case of null parent scenario.
        select 
            null as scenarioid,
            pl.* exclude (sharefactor, premiumfactor),
            pl.sharefactor,
            pl.premiumfactor,
            null as notes
        from
            economic_model_staging.portlayer pl;


    -- retroconfiguration
    create or replace table economic_model_scenario.retroconfiguration_scenario as
        select distinct
            sp.scenarioid, 
            l.retroconfigurationid,
            l.retrocontractid,
            l.startdate,
            l.targetcollateral as targetcollateralrevo,
            last_value(l_o.targetcollateraloverride) ignore nulls over (partition by sp.scenarioid, l.retroconfigurationid order by depth asc) targetcollateraloverride
        from
            economic_model_staging.retroconfiguration l
            cross join economic_model_scenario.scenario_parts sp
            left outer join economic_model_scenario.retroconfiguration_override l_o on sp.partid = l_o.scenarioid and l.retroconfigurationid = l_o.retroconfigurationid
        union
        select 
            null as scenarioid, 
            retroconfigurationid,
            retrocontractid,
            startdate,
            targetcollateral,
            null
        from 
            economic_model_staging.retroconfiguration
        ;

    
    -- retroinvestmentleg
    create or replace table economic_model_scenario.retroinvestmentleg_scenario as
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
            )
            select 
                * exclude (investmentsigned_original, investmentsignedamt_original),
                economic_model_computed.concat_non_null(
                    economic_model_scenario.compare_and_note(investmentsigned, investmentsigned_original, 'InvestmentSigned'),
                    economic_model_scenario.compare_and_note(investmentsignedamt, investmentsignedamt_original, 'InvestmentsignedAmt')
                ) AS notes
            from 
                withResolvedAndOriginalValues
        union 
        select
            null scenarioid,
            l.retroinvestmentlegid,
            l.retroconfigurationid,
            l.retrocontractinvestorid,
            l.investmentsigned,
            l.investmentsignedamt,
            null as investmentcalculatedpct,
            null as notes
        from
            economic_model_staging.retroinvestmentleg l
            ;

end
;