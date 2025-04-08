CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_SCENARIO.RESOLVE_SCENARIO_DATA_2()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
$$
begin

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
    create or replace temporary table economic_model_scenario.retrocontract_scenario_tmp as
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
        select distinct
            sp.scenarioid, 
            rc.* exclude (commissiononnetpremium, level, isactive, profitcommissionpctofprofit, ReinsuranceBrokerageOnNetPremium, ReinsuranceExpensesOnCededCapital, ReinsuranceExpensesOnCededPremium),
            coalesce(last_value(rc_o.isactive) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.isactive) isactive,
            coalesce(last_value(rc_o.level) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.level) level,
            coalesce(last_value(rc_o.commission) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.commissiononnetpremium) commission,
            coalesce(last_value(rc_o.profitcommission) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.profitcommissionpctofprofit) profitcommission,
            coalesce(last_value(rc_o.ReinsuranceBrokerageOnNetPremium) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.ReinsuranceBrokerageOnNetPremium) ReinsuranceBrokerageOnNetPremium,
            coalesce(last_value(rc_o.ReinsuranceExpensesOnCededCapital) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.ReinsuranceExpensesOnCededCapital) ReinsuranceExpensesOnCededCapital,
            coalesce(last_value(rc_o.ReinsuranceExpensesOnCededPremium) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc), rc.ReinsuranceExpensesOnCededPremium) ReinsuranceExpensesOnCededPremium,
            last_value(case when irc.retrocontractid is null then false else true end) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc) includeInAnalysis,
            last_value(case when rc_o.retrocontractid is null then false else true end) ignore nulls over (partition by sp.scenarioid, rc.retrocontractid order by depth asc) HasOverride,
        from
            economic_model_revoext.retrocontract rc
            cross join economic_model_scenario.scenario_parts sp
            left outer join economic_model_scenario.retrocontract_override rc_o on sp.partid = rc_o.scenarioid and rc.retrocontractid = rc_o.retrocontractid
            left outer join scenarioRetroContracts irc on irc.retrocontractid = rc.retrocontractid and irc.scenarioid = sp.partid
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
            false HasOverride
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
    create or replace temporary table economic_model_scenario.portlayer_scenario_tmp as
        with scenarioPortfolios as (
            select 
                scenarioid, portfolioid
            from 
                economic_model_scenario.scenario s
                cross join table(split_to_table(coalesce(s.analysis_PORTFOLIOIDS, ''), ',')) t
                inner join economic_model_staging.portfolio p on trim(t.value) = p.portfolioid
        )
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
            end premiumfactor
        from
            economic_model_staging.portlayer pl
            cross join economic_model_scenario.scenario_parts sp
            -- limit to portfolios included in scenario (todo: parts and main scenario should have the same list of portfolios)
            inner join scenarioPortfolios pf on pl.portfolioid = pf.portfolioid and pf.scenarioid = sp.scenarioid
            left outer join economic_model_scenario.portlayer_override pl_o on sp.partid = pl_o.scenarioid and pl.portlayerid = pl_o.portlayerid
            left outer join economic_model_scenario.topupzone_override_unpivoted tz_u_o on sp.partid = tz_u_o.scenarioid and pl.topupzoneid = tz_u_o.topupzoneid and tz_u_o.productgroup = pl.productgroup
        union
        -- include the base values too. This is primarily needed in the scenarioeditor which has to show the parent scenario values or the original values in case of null parent scenario
        select 
            null as scenarioid,
            pl.* exclude (sharefactor, premiumfactor),
            pl.sharefactor,
            pl.premiumfactor
        from
            economic_model_staging.portlayer pl;



    -- retroconfiguration
    create or replace temporary table economic_model_scenario.retroconfiguration_scenario_tmp as
        select distinct
            sp.scenarioid, 
            l.retroconfigurationid,
            l.retrocontractid,
            l.startdate,
            l.targetcollateral as targetcollateralrevo,
            last_value(l_o.targetcollateraloverride) ignore nulls over (partition by sp.scenarioid, l.retroconfigurationid order by depth asc) targetcollateraloverride,
            last_value(l_o.targetcollateralcalculated) ignore nulls over (partition by sp.scenarioid, l.retroconfigurationid order by depth asc) targetcollateralcalculated
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
            null,
            null
        from 
            economic_model_staging.retroconfiguration
        ;

    
    -- retroinvestmentleg
    create or replace temporary table economic_model_scenario.retroinvestmentleg_scenario_tmp as
       select distinct
            sp.scenarioid, 
            l.* exclude (investmentsigned, investmentsignedamt),
            coalesce(last_value(l_o.INVESTMENTSIGNEDPCT) ignore nulls over (partition by sp.scenarioid, l.retroinvestmentlegid order by depth asc), l.investmentsigned) investmentsigned,
            coalesce(last_value(l_o.INVESTMENTSIGNEDAMT) ignore nulls over (partition by sp.scenarioid, l.retroinvestmentlegid order by depth asc), l.investmentsignedamt) investmentsignedamt,
            last_value(l_o.investmentsignedpctcalculated) ignore nulls over (partition by sp.scenarioid, l.retroinvestmentlegid order by depth asc) investmentsignedpctcalculated
        from
            economic_model_staging.retroinvestmentleg l
            cross join economic_model_scenario.scenario_parts sp
            left outer join economic_model_scenario.retroinvestmentleg_override l_o on sp.partid = l_o.scenarioid and l.retroinvestmentlegid = l_o.retroinvestmentlegid
        union 
        select
            null scenarioid,
            l.retroinvestmentlegid,
            l.retroconfigurationid,
            l.retrocontractinvestorid,
            l.investmentsigned,
            l.investmentsignedamt,
            null as investmentsignedpctcalculated
        from
            economic_model_staging.retroinvestmentleg l
            ;

    // =================================================================================================================
    // ======= Change detection - detecting whcih retros (and portlayers - for gross blocks) need recalculation ========
    // =================================================================================================================
    
    create or replace temporary table economic_model_scenario.RetrosForRecalc_tmp (scenarioid int, retrocontractid string, includeDependents boolean);
    create or replace temporary table economic_model_scenario.PortLayersForRecalc_tmp (scenarioid int, portlayerid string);

    // A: find portlayers with changes
    insert into economic_model_scenario.PortLayersForRecalc_tmp
        select distinct
            pl.portlayerid,
            sc.scenarioid
        from 
            economic_model_staging.portlayer pl
            cross join economic_model_scenario.scenario sc
            left join economic_model_scenario.portlayer_scenario old on old.portlayerid = pl.portlayerid and old.scenarioid = sc.scenarioid
            left join economic_model_scenario.portlayer_scenario_tmp new on new.portlayerid = pl.portlayerid and new.scenarioid = sc.scenarioid
        where 
            sc.isactive = 1
            and 
            (
                old.sharefactor <> new.sharefactor or
                old.premiumfactor <> new.premiumfactor 
            )
        ;
    
    // B: find retros that need change

    // B0: we'll need to know which retros are volatile
    -- retros which change cession % during reclculation are volatile 
    -- (they use the calculated target collateral so the cession % changes, so dependents will need recalculation as well)
    create temporary table economic_model_scenario.volatileRetros as
        select distinct
            sc.scenarioid,
            rc.retrocontractid,
            max_by(
                (
                    coalesce(old.targetcollateralrevo, old.targetcollateraloverride) is null OR 
                    coalesce(new.targetcollateralrevo, new.targetcollateraloverride) is null
                )
                , rc.startdate
            ) isVolatile
        from 
            economic_model_staging.retroconfiguration rc
            cross join economic_model_scenario.scenario sc
            left join economic_model_scenario.retroconfiguration_scenario old on rc.retroconfigurationid = old.retroconfigurationid and old.scenarioid = sc.scenarioid
            left join economic_model_scenario.retroconfiguration_scenario_tmp new on rc.retroconfigurationid = new.retroconfigurationid and new.scenarioid = sc.scenarioid
        where 
            sc.isactive = 1
        group by
            sc.scenarioid, rc.retrocontractid
    ;
    
    -- B1: retros whose parameters have changed (might require recalc of dependets, but not always, 
    -- e.g. if revo or override capital are set and the change is to commission and not to gross or signed cession %,
    -- in which case, the changes are limited to just the retro in question).
    insert into economic_model_scenario.RetrosForRecalc_tmp
        select 
            sc.scenarioid,
            r.retrocontractid, 
            // dependes need to be recalculated if the retro is volatile, or we're changing level or isactive
            (
                isvolatile or 
                old.Level <>  new.Level or
                old.IsActive <> new.IsActive
            ) as includedependents
        from 
            economic_model_revoext.retrocontract r
            cross join economic_model_scenario.scenario sc
            left join economic_model_scenario.retrocontract_scenario old on r.retrocontractid = old.retrocontractid and old.scenarioid = sc.scenarioid
            left join economic_model_scenario.retrocontract_scenario_tmp new on r.retrocontractid = new.retrocontractid and new.scenarioid = sc.scenarioid
            left join economic_model_scenario.volatileRetros vr on vr.retrocontractid = r.retrocontractid and vr.scenarioid = sc.scenarioid
        where
            sc.isactive = 1
            and 
            (
                zeroifnull(old.Commission) <>  zeroifnull(new.Commission) or
                zeroifnull(old.ProfitCommission) <>  zeroifnull(new.ProfitCommission) or
                zeroifnull(old.ReinsuranceBrokerageOnNetPremium) <>  zeroifnull(new.ReinsuranceBrokerageOnNetPremium) or
                zeroifnull(old.ReinsuranceExpensesOnCededCapital) <>  zeroifnull(new.ReinsuranceExpensesOnCededCapital) or
                zeroifnull(old.ReinsuranceExpensesOnCededPremium) <>  zeroifnull(new.ReinsuranceExpensesOnCededPremium) or
                old.Level <> new.Level or
                old.IsActive <> new.IsActive
            )
            ;

    // B2: retros whose target gross (portlayers) have changed
    insert into economic_model_scenario.RetrosForRecalc_tmp
        select 
            distinct plr.scenarioid, retrocontractid, vr.isvolatile
        from 
            economic_model_staging.retrotag t
            inner join economic_model_staging.portlayerperiod per on t.periodid = per.periodid
            inner join economic_model_staging.retroconfiguration rc on t.retroconfigurationid = rc.retroconfigurationid
            inner join economic_model_scenario.PortLayersForRecalc_tmp plr on plr.portlayerid = per.portlayerid
            left join economic_model_scenario.volatileRetros vr on vr.retrocontractid = rc.retrocontractid and vr.scenarioid = plr.scenarioid
    ;

    // B3: retros whose investment % has changed
    -- Here we might have to account for dependent retros, because if we hit a volatile retros, it will change its cession %
    -- which will impact it's dependents even if those dependents don't have the portlayer that caused the change.
    insert into economic_model_scenario.RetrosForRecalc_tmp
        select 
            distinct sc.scenarioid, rc.retrocontractid, true
        from 
            economic_model_staging.retroinvestmentleg rl
            inner join economic_model_staging.retroconfiguration rc on rl.retroconfigurationid = rc.retroconfigurationid
            cross join economic_model_scenario.scenario sc
            left outer join economic_model_scenario.retroinvestmentleg_scenario old on old.retroinvestmentlegid = rl.retroinvestmentlegid and old.scenarioid = sc.scenarioid
            left outer join economic_model_scenario.retroinvestmentleg_scenario_tmp new on new.retroinvestmentlegid = rl.retroinvestmentlegid and new.scenarioid = sc.scenarioid
        where 
            sc.isactive = 1 
            and zeroifnull(old.INVESTMENTSIGNED) <> zeroifnull(new.INVESTMENTSIGNED)
    ;

    // C: apply findings to scenario resolved tables

    -- C1: portlayer_s.requiresrecald
    alter table economic_model_scenario.portlayer_scenario
    add column RequiresRecalc boolean;

    update 
        economic_model_scenario.portlayer_scenario pls
    set
       RequiresRecalc = true
    from 
        economic_model_scenario.PortLayersForRecalc_tmp plr
    where
        pls.portlayerid = plr.portlayerid 
        and pls.scenarioid = plr.scenarioid;

    -- C2: retrocontract_s.requiresrecald
    alter table economic_model_scenario.retrocontract_scenario
    add column RequiresRecalc boolean;
    
    update 
        economic_model_scenario.retrocontract_scenario rs
    set 
        requiresrecalc = true
    from (
        -- include all changed retros
        select distinct
            scenarioid, retrocontractid
        from 
            economic_model_scenario.RetrosForRecalc_tmp
        union 
        -- for dependent retros, we're using a simplified approach: recalculate all retros above the volatile changed ones. 
        -- it's possibly simpler to calculate the metrics for a few extra retros that to find out which retro actually depends on which lower level one.
        select distinct
            rs.scenarioid, rs.retrocontractid
        from 
            economic_model_scenario.RetrosForRecalc_tmp tmp
            inner join economic_model_scenario.retroconfiguration_scenario rs on tmp.level < rs.level and tmp.scenarioid = rs.scenarioid
        where 
            tmp.includedependents = true
            and tmp.isactive = true
            and rcs.isactive = true
    ) x
    where
        rs.retrocontractid = x.retrocontractid and rs.scenarioid = x.scenarioid;
            
    // D: apply new data and clean up temp tables
    create or replace temporary table economic_model_scenario.retrocontract_scenario as
    select * from economic_model_scenario.retrocontract_scenario_tmp;
    drop table economic_model_scenario.retrocontract_scenario_tmp;
   
    create or replace temporary table economic_model_scenario.portlayer_scenario as
    select * from economic_model_scenario.portlayer_scenario_tmp;  
    drop table economic_model_scenario.portlayer_scenario_tmp;

    create or replace temporary table economic_model_scenario.retroconfiguration_scenario as
    select * from economic_model_scenario.retroconfiguration_scenario_tmp;
    drop table economic_model_scenario.retroconfiguration_scenario_tmp;

    create or replace temporary table economic_model_scenario.retroinvestmentleg_scenario as
    select * from economic_model_scenario.retroinvestmentleg_scenario_tmp;
    drop table economic_model_scenario.retroinvestmentleg_scenario_tmp;

end
$$;