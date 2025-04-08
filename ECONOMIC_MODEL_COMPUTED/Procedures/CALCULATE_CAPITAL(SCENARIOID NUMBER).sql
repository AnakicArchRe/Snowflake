CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.CALCULATE_CAPITAL(SCENARIOID NUMBER)
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
$$
begin

    -- TODO: we're doing the same joins (starting with RetroTag) over and over in this stored procedure... 
    -- It would likely perform better if we join all used data at block level and save it as a temp table which we then use for additional joins and groupings.

    -- 1. prepare visible scenarios
    create or replace temporary table economic_model_computed.ScenarioFiltered as
        select
            *
        from 
            economic_model_scenario.scenario 
        where
            isactive = 1
            -- and scenarioid = 7470 //temp: testing
            and (scenarioid = :scenarioId OR :scenarioId is null)
        ;

    -- 2. We'll need to know how much each block is ceding to investors
    create or replace temporary table economic_model_computed.retroInvestorBlock_GrossPct(scenarioid int, retroblockid string, retrocontractinvestorid string, cessiongross float);
    
    -- 2.a. We can calculate the gross pct for retroblocks where retro.IsSpecific=true right away as they do not vary by scenario (they are inforce).
    -- Note that we do a cross join with scenario just to have rows for each scenario.
    insert into economic_model_computed.retroInvestorBlock_GrossPct(scenarioid, retroblockid, retrocontractinvestorid, cessiongross)
        select
            sc.scenarioid, 
            -- todo: rename column in retrotag table
            -- todo: do we need this column? I don't think we ever use it for joins or anything.
            rb.retroblockid,
            rci.retrocontractinvestorid,
            -- todo: It looks like some retros with IsSpecific=1 have multiple investors for a given layer. Is this expected? Investigate.
            rb.placement * ra.cessiongross as CessionGross
        from 
            economic_model_staging.retrotag rb
            inner join economic_model_staging.RetroConfiguration rpc on rpc.RetroConfigurationid = rb.RetroConfigurationid
            inner join economic_model_staging.portlayerperiod lp on lp.periodid = rb.periodid
            cross join economic_model_computed.scenariofiltered sc
            inner join economic_model_scenario.portlayer_scenario pl on pl.portlayerid = lp.portlayerid and pl.scenarioid = sc.scenarioid
            inner join economic_model_scenario.retroinvestmentleg_scenario rci on rci.retroconfigurationid = rpc.retroconfigurationid and rci.scenarioid = sc.scenarioid
            inner join economic_model_scenario.retrocontract_scenario r on rpc.retrocontractid = r.retrocontractid and r.scenarioid = sc.scenarioid
            inner join economic_model_staging.retroallocation ra on ra.layerid = pl.layerid and ra.retrocontractinvestorid = rci.retrocontractinvestorid
        where
            r.isspecific = 1 and r.isactive = 1;

    -- 3. go through retrocontracts one level at a time and calculate the required capital 
    -- based on the data in that level as well the remaining % after the previous level cessions.
    
    -- in the loop, we'll need to keep track of how much of each portlayer is available at each cession level in each period (this varies by scenario)
    create or replace temporary table economic_model_computed.PeriodLevelAvailable
    (
        scenarioid int, 
        level int, 
        periodid string, 
        available float, 
        explanation string
    );
          
    declare
        currLevel int;
        lvlCurr cursor for select seq4()+1 as level from table(generator(rowcount => 5));
    begin
    
        FOR lvlRow IN lvlCurr DO

            -- todo: check lvl 3 retros (retroleveltype=2), I think they should have -sign. Ask PC to verify if the same calculation applies to them.
            currLevel := lvlRow.level;

            create or replace temporary table economic_model_computed.retroConfigCalculatedCapital as 
                with 
                    lvl_available as (
                        select 
                            periodid, 
                            scenarioid,
                            max_by(available, level) as available
                        from
                            economic_model_computed.PeriodLevelAvailable pla
                        where 
                            pla.level <= :currLevel
                        group by 
                            periodid, 
                            scenarioid
                    )
                    , subjectBlocksUSD as (
                        select
                            pls.scenarioid,
                            per.yeltperiodid,
                            per.periodid,
                            pls.portlayerid,
                            rc.retrocontractid,
                            coalesce(la.available, 1) as availableAtLevel,
                            availableAtLevel * pls.limit100pct * pls.share * pls.sharefactor * b.placement * fx.rate as exposedLimit,
                            availableAtLevel * pls.premium100pct * pls.share * pls.sharefactor * pls.premiumfactor * b.placement * fx.rate as exposedPremium,
                            // todo: switch to seasonal premium calculation
                            availableAtLevel * pls.premium100pct * pls.share * pls.sharefactor * pls.premiumfactor * b.placement * per.shareoflayerduration * fx.rate as proRataPremium,
                            availableAtLevel * pls.premium100pct * pls.share * pls.sharefactor * pls.premiumfactor * b.placement * per.shareoflayerduration * fx.rate * pls.expenses as proRataPremiumExpenses
                        from 
                            economic_model_staging.retrotag b
                            inner join economic_model_staging.retroconfiguration rc on b.retroconfigurationid = rc.retroconfigurationid
                            inner join economic_model_staging.portlayerperiod per on b.periodid = per.periodid
                            inner join economic_model_scenario.portLayer_scenario pls on per.portlayerid = pls.portlayerid
                            -- using reference portfolios to calculate required capital for retro contract
                            inner join economic_model_computed.scenariofiltered sf on pls.scenarioid = sf.scenarioid
                            inner join economic_model_staging.submission s on s.submissionid = pls.submissionid
                            inner join economic_model_staging.fxrate fx 
                                on fx.currency = s.currency 
                                and fx.basecurrency = 'USD'
                                -- try to use fxdate from scenario, fallback to using submission fxdate
                                and fx.fxdate = coalesce(sf.fxdate, s.fxdate)
                            inner join economic_model_scenario.retrocontract_scenario rps on rps.scenarioid = sf.scenarioid and rps.retrocontractid = rc.retrocontractid
                            left join lvl_available la on b.periodid = la.periodid and la.scenarioid = sf.scenarioid
                            -- todo: join to retrocontract (program*<->*contract) so we can distribute the blocks to contracts and we can group them later by contract
                        where 
                            rps.isactive = 1
                            // only look at blocks that start in the exposure period
                            // todo: check with PC if we should filter by portlayerinception instead? (I think this would be ok for RAD, but not for LOD, so that's why I'm using block.periodstart)
                            and rps.exposureStart <= per.periodstart and per.periodstart <= rps.exposureend
                            and rps.level = :currLevel
                    )
                    , blockPremiums as (
                        select 
                            scenarioid,
                            portlayerid,
                            retrocontractid,
                            sum(proratapremium) proRataPremium,
                            sum(proratapremiumexpenses) proRataPremiumExpenses
                        from 
                            subjectBlocksUSD
                        group by 
                            -- grouping across all periods
                            scenarioid,
                            portlayerid,
                            retrocontractid
                    )
                    , blockYlt as (
                        select
                            b.scenarioid,
                            portlayerid,
                            b.retrocontractid,
                            year,  
                            lossviewgroup,
                            sum(totalloss * exposedlimit * coalesce(rcs.nonmodeledload, 1) * least(coalesce(rcs.climateload, 1), y.maxlossscalefactor)) as subjectLossUsd,
                            sum(totalrb * exposedpremium * coalesce(rcs.nonmodeledload, 1) * least(coalesce(rcs.climateload, 1), y.maxlossscalefactor)) as subjectRbUsd,
                            sum(totalrp * exposedpremium * coalesce(rcs.nonmodeledload, 1) * least(coalesce(rcs.climateload, 1), y.maxlossscalefactor)) as subjectRpUsd
                        from 
                            subjectBlocksUSD b
                            inner join economic_model_staging.yelpt y on b.yeltperiodid = y.yeltperiodid
                            inner join economic_model_scenario.retrocontract_scenario rcs on b.retrocontractid = rcs.retrocontractid and b.scenarioid = rcs.scenarioid
                        where 
                            lossviewgroup = rcs.capitalcalculationlossview
                        group by 
                            b.scenarioid,
                            year,
                            lossviewgroup,
                            b.retrocontractid,
                            portlayerid
                    )
                    , blockYltAndBaseData as (
                        select 
                            y.*, 
                            p.proRataPremium, 
                            p.proRataPremiumexpenses
                        from 
                            blockYlt y
                            inner join blockPremiums p on y.portlayerid = p.portlayerid and p.scenarioid = y.scenarioid and p.retrocontractid = y.retrocontractid
                    )
                    , retroContractResults as (
                        select 
                            b.scenarioid,
                            year, 
                            b.retrocontractid,
                            sum(proRataPremium) as premium,
                            sum(proRataPremiumexpenses) as expenses,
                            sum(subjectlossusd) as losses,
                            sum(subjectrpusd) as rp,
                            sum(subjectrbusd) as rb,
                            premium - expenses + rp - rb as netPremium,
                            rcs.REINSURANCEBROKERAGEONNETPrEMIUM as brokerageOnPreium,
                            rcs.commission as commissionOnPremium,
                            CAPITALCALCULATIONTARGETRETURNPERIOD,
                            netPremium * (1 - brokerageOnPreium - commissionOnPremium) as availablePremiumForLosses,
                            losses - availablePremiumForLosses as netresult,
                            greatest(0, netresult) as shortfall,
                            rank() over (partition by b.retrocontractid order by netresult desc) rnk
                        from 
                            blockYltAndBaseData b
                            inner join economic_model_scenario.retrocontract_scenario rcs on b.retrocontractid = rcs.retrocontractid and b.scenarioid = rcs.scenarioid
                        group by 
                            b.scenarioid,
                            year,
                            b.retrocontractid,
                            rcs.REINSURANCEBROKERAGEONNETPrEMIUM,
                            rcs.commission,
                            CAPITALCALCULATIONTARGETRETURNPERIOD
                    )            
                    , retroRequiredCapital as (
                        select 
                            scenarioid, retrocontractid, round(avg(shortfall)) requiredCapital
                        from 
                            retroContractResults
                        where
                            rnk <= CAPITALCALCULATIONTARGETRETURNPERIOD
                        group by 
                            scenarioid, retrocontractid
                    )
                    select 
                        rqc.scenarioid, 
                        rqc.retrocontractid, 
                        rqc.requiredCapital, 
                        -- find latest retroconfiguration and apply the calculation to it
                        max_by(rc.retroconfigurationid, rc.startdate) as retroconfigurationid
                    from
                        retroRequiredCapital rqc
                        inner join economic_model_staging.retroconfiguration rc on rqc.retrocontractid = rc.retrocontractid
                    group by 
                        rqc.scenarioid, 
                        rqc.retrocontractid, 
                        rqc.requiredCapital;

            -- update calculated required capital for each scenario
            MERGE INTO
                economic_model_scenario.retroconfiguration_override rc_o 
            USING 
                economic_model_computed.retroConfigCalculatedCapital AS reqCap ON reqCap.retroconfigurationid = rc_o.retroconfigurationid and reqCap.scenarioid = rc_o.scenarioid
            WHEN MATCHED THEN 
                 UPDATE SET rc_o.targetcollateralcalculated = reqCap.requiredCapital
            WHEN NOT MATCHED THEN 
                 INSERT (retroconfigurationid, scenarioid, targetcollateralcalculated) VALUES (reqCap.retroconfigurationid, reqCap.scenarioid, reqCap.requiredcapital)
            ;

            -- update calculated investment % for each investor
            -- todo: see if I need to handle retros with IsSpecific=true differently as they will have InvestmentSigned=1 and the actual % will be in the retroallocation table.
            -- run the query (in the using block) and see if it makes changes to retros with IsSpecific and if they are ok.
            MERGE INTO
                economic_model_scenario.retroinvestmentleg_override rci_o
            using
            (
                select
                    rci.retroinvestmentlegid, 
                    rcc.scenarioid, 
                    rci.investmentsignedamt / rcc.requiredcapital as investmentsignedcalculated 
                from 
                    economic_model_computed.retroConfigCalculatedCapital rcc
                    inner join economic_model_scenario.retroinvestmentleg_scenario rci on rcc.retroconfigurationid = rci.retroconfigurationid and rcc.scenarioid = rci.scenarioid
                where 
                    investmentsignedamt > 0
            ) as invShareOvrd on
                invShareOvrd.retroinvestmentlegid = rci_o.retroinvestmentlegid and invShareOvrd.scenarioid = rci_o.scenarioid
            WHEN MATCHED THEN 
                 UPDATE SET rci_o.investmentsignedpctcalculated = invShareOvrd.investmentsignedcalculated
            WHEN NOT MATCHED THEN 
                 INSERT (retroinvestmentlegid, scenarioid, investmentsignedpctcalculated) VALUES (invShareOvrd.retroinvestmentlegid, invShareOvrd.scenarioid, invShareOvrd.investmentsignedcalculated);
        
            -- Record how much we're ceding to each block at this level.
            -- We need this for "available" in the next level, as well as for calculating cession results for investors
            insert into economic_model_computed.retroInvestorBlock_GrossPct(scenarioid, retroblockid, retrocontractinvestorid, cessiongross)
                 select
                    r.scenarioid, 
                    rb.retroblockid,
                    rci.retrocontractinvestorid,
                    -- todo: It looks like some retros with IsSpecific=1 have multiple investors for a given layer. Is this expected? Investigate.
                    -- note: we use the investmentsigned specified by the overide, or the calculated one if override not found
                    -- todo: does REVO take precedence over the scenario editor? Always or in some cases?
                    rb.placement * coalesce(rci.investmentsigned, rci.investmentsignedpctcalculated) as CessionGross
                from 
                    economic_model_staging.retrotag rb
                    inner join economic_model_staging.RetroConfiguration rpc on rpc.RetroConfigurationid = rb.RetroConfigurationid
                    inner join economic_model_staging.portlayerperiod lp on lp.periodid = rb.periodid
                    inner join economic_model_scenario.retroinvestmentleg_scenario rci on rci.retroconfigurationid = rpc.retroconfigurationid
                    inner join economic_model_scenario.retrocontract_scenario r on rpc.retrocontractid = r.retrocontractid and rci.scenarioid = r.scenarioid
            
                    // we calculate cession information for refereceportfolios only
                    inner join economic_model_scenario.portlayer_scenario pl on pl.portlayerid = lp.portlayerid and pl.scenarioid = r.scenarioid
                where
                    isspecific = 0
                    and CessionGross > 0
                    and r.level = :currLevel
            ;
        
            -- add "available" information for next level to all periods/scenarios covered at this level
            insert into economic_model_computed.PeriodLevelAvailable(scenarioid, periodid, level, available, explanation)
            with levelGross as (
                select 
                    rps.scenarioid,
                    rps.level,
                    b.periodid,
                    sum(coalesce(rib.cessiongross, 0)) as levelGrossPct,
                    listagg(rps.retrocontractid || ': ' || round(100 * coalesce(rib.cessiongross, 0), 2) || '%', ', ') description
                from 
                    economic_model_staging.retrotag b
                    inner join economic_model_staging.portlayerperiod p on b.periodid = p.periodid
                    inner join economic_model_staging.retroconfiguration rc on b.retroconfigurationid = rc.retroconfigurationid
                    inner join economic_model_scenario.portlayer_scenario pls on p.portlayerid = pls.portlayerid
                    inner join economic_model_scenario.retrocontract_scenario rps on rc.retrocontractid = rps.retrocontractid and pls.scenarioid = rps.scenarioid
                    left join economic_model_computed.retroInvestorBlock_GrossPct rib on rib.retroblockid = b.retroblockid and rib.scenarioid = rps.scenarioid
                where 
                    rps.isactive = 1
                    and rps.level = :currLevel
                group by 
                    rps.scenarioid,
                    rps.level,
                    b.periodid
            )
            , levelAvailable as (
                select 
                    scenarioid, 
                    periodid,
                    max_by(available, level) as available,
                    max_by(explanation, level) as explanation
                from
                    economic_model_computed.PeriodLevelAvailable
                 where
                    level <= :currLevel
                group by 
                    scenarioid, periodid
            )
            , res as (
                select 
                    g.scenarioid, 
                    g.periodid, 
                    g.level + 1 as level, 
                    coalesce(plv.available, 1) * (1 - g.levelGrossPct) as available, 
                    concat('RLT_', g.level, ':', round(100 * g.levelGrossPct,2), '%  => ', g.description, '\n', coalesce(plv.explanation, '')) as explanation
                from
                    levelgross g
                    left join levelAvailable plv on g.scenarioid = plv.scenarioid and g.periodId = plv.periodid
                where
                    levelGrossPct > 0
            )
            select * from res
            -- we don't need to insert if nothing has been ceded (we have coalesce(,1) to deal with that)
            where available <> 1;
    
        END FOR;
    end;

    // todo: we really just need to resolve the "retroconfiguration_scenario" and "retroinvestmentleg_scenario" tables, so we should break up this stored procedure into many, and just canll the ones we need. 
    // resolving portlayers takes a long time, and we don't need that. Or add parameter(s) for what to refresh.
    call economic_model_scenario.resolve_scenario_data();
    
end
$$;