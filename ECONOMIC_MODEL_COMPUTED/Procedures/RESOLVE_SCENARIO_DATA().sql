CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.RESOLVE_SCENARIO_DATA()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    -- scenario parts
    create or replace table economic_model_scenario.scenario_parts as
        with recursive cte as (
            // each scenario consists at least of itself
            select 
                sf.scenarioid,
                sf.scenarioid as partId,
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
                cte.hops + 1 as hops
            from 
                economic_model_scenario.scenario sf
                inner join cte on sf.parentscenarioid = cte.scenarioid
            where 
                sf.isactive = 1
        )
        select 
            *, 
            count(*) over (partition by scenarioid) - hops as depth 
        from 
            cte 
        order by 
            scenarioid, partid;

    
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
    
    -- need to know which portfolios are visible in which scenario, so we can filter out portlayers that aren't part of them
    -- and retro entities that don't interact with them
    create or replace temporary table economic_model_computed.portfolio_scenario as
        select 
            scenarioid, p.*
        from 
            economic_model_scenario.scenario s
            cross join table(split_to_table(coalesce(s.analysis_PORTFOLIOIDS, ''), ',')) t
            inner join economic_model_staging.portfolio p on trim(t.value) = p.portfolioid
    ;

    -- portlayer
    create or replace table economic_model_computed.portLayer_scenario as
        with withResolvedAndOriginalValues as (
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
                pl.premiumfactor as premiumFactor_original,
            from
                economic_model_staging.portlayer pl
                cross join economic_model_scenario.scenario_parts sp
                -- limit to portfolios included in scenario
                inner join economic_model_computed.portfolio_scenario pf on pl.portfolioid = pf.portfolioid and pf.scenarioid = sp.partid
                left outer join economic_model_scenario.portlayer_override pl_o on sp.partid = pl_o.scenarioid and pl.portlayerid = pl_o.portlayerid
                left outer join economic_model_scenario.topupzone_override_unpivoted tz_u_o on sp.partid = tz_u_o.scenarioid and pl.topupzoneid = tz_u_o.topupzoneid and tz_u_o.productgroup = pl.productgroup
        )
        select 
            x.* exclude (shareFactor_original, premiumFactor_original, limit100pct, premium100pct, boundfxdate),
            -- note: if the scenario attempts to lock in the boundfx, then use the boundfx of the layer if available (for inforce) and use
            -- the scenario fxdate as a fallback (for projected). If the scenario doesn't lock in boundfx, use scenario.fxdate for all layers.
            limit100pct * fx.rate as limit100Pct,
            premium100pct * fx.rate as premium100pct,
            iff(sc.boundfxlockin, coalesce(x.boundFxDate, sc.fxdate), sc.fxdate) used_fx_date,
            fx.rate as used_fx_rate,
            s.currency as original_currency,
            limit100pct as limit100pct_original_currency, 
            premium100pct as premium100pct_original_currency,
            economic_model_computed.concat_non_null(
                economic_model_computed.compare_and_note(sharefactor, sharefactor_original, 'ShareFactor'),
                economic_model_computed.compare_and_note(premiumfactor, premiumfactor_original, 'PremiumFactor')
            ) AS notes
        from 
            withResolvedAndOriginalValues x
            inner join economic_model_scenario.scenario sc on x.scenarioid = sc.scenarioid
            inner join economic_model_staging.submission s on x.submissionid = s.submissionid
            inner join economic_model_staging.fxrate fx on s.currency = fx.currency and used_fx_date = fx.fxdate and fx.basecurrency = 'USD'
        ;

    -- filter out retros that don't impact a scenario (don't interact with portfolios it contains)
    create or replace temporary table economic_model_computed.cessionflag as 
        select distinct
            p.scenarioid, retrocontractid
        from 
            economic_model_computed.portfolio_scenario p
            inner join economic_model_staging.portlayer pl on p.portfolioid = pl.portfolioid
            inner join economic_model_staging.portlayerperiod per on pl.portlayerid = per.portlayerid
            inner join economic_model_staging.retrotag t on per.periodid = t.periodid
            inner join economic_model_staging.retroconfiguration rc on t.retroconfigurationid = rc.retroconfigurationid;

    -- retrocontract
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

        
    -- retroconfiguration
    create or replace table economic_model_computed.retroconfiguration_scenario as
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
            -- make sure the retro interacts with portfolios visible in the scenario
            inner join economic_model_computed.cessionflag f on l.retrocontractid = f.retrocontractid and sp.partid = f.scenarioid
        ;

    
    -- retroinvestmentleg
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

end
;