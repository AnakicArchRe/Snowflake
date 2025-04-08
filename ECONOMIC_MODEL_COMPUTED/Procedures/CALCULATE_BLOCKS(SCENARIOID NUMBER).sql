CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.CALCULATE_BLOCKS(SCENARIOID NUMBER)
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
$$
begin

    -- Find scenarios to recalculate. Since we're computing diff blokcs, we have to include selected and derived scenarios.
  

    -- 2. Calculate cession gross % for each investor block
    create or replace temporary table economic_model_computed.retroInvestorBlock_GrossPct as
        select
            sc.scenarioid,
            rb.retroblockid,
            rci.retrocontractinvestorid,
            -- todo: I noticed that some retros with IsSpecific=1 have multiple investors for a given layer. Check to make sure if this is expected.
            -- note: attempt to use 1) overriden investmentsigned from the scenario editor if available, then 2) revo and finally 3) the calculated share
            -- todo: verify priorities with PC. Does REVO override scenario editor? (e.g. infoce > scenario... if so, is this the case just with past ones or projections as well? can projections have revo data?)
            rb.placement * case when r.isspecific =1 then ra.cessiongross else coalesce(rci.investmentsigned, rci.investmentsignedpctcalculated) end as CessionGross,
            -- For performance reasons including these two columns right away. Two joins less in the next step seem to have a non trivial impact in perf. 
            -- Normally we'd look them up by retroblockid but this table can have ~0.5-1Bn rows, so we want to reduce joins to it as much as possible.
            lp.periodid,
            r.retrocontractid,
            r.level
        from 
            economic_model_staging.retrotag rb
            inner join economic_model_staging.RetroConfiguration rpc on rpc.RetroConfigurationid = rb.RetroConfigurationid
            inner join economic_model_staging.portlayerperiod lp on lp.periodid = rb.periodid
            cross join economic_model_computed.scenariostocompute sc
            inner join economic_model_scenario.portlayer_scenario pl on pl.portlayerid = lp.portlayerid and pl.scenarioid = sc.scenarioid
            inner join economic_model_scenario.retroinvestmentleg_scenario rci on rci.retroconfigurationid = rpc.retroconfigurationid and rci.scenarioid = sc.scenarioid
            inner join economic_model_scenario.retrocontract_scenario r on rpc.retrocontractid = r.retrocontractid and r.scenarioid = sc.scenarioid
            // left join because there will not be a retroallocation for projeted layers, this should only happen for retro with IsSpecific=1
            left join economic_model_staging.retroallocation ra on ra.layerid = pl.layerid and ra.retrocontractinvestorid = rci.retrocontractinvestorid
        where
            r.isactive = 1
        ;


    -- 3. For each period, calculate how much we're ceding to each LEVEL in each scenario

    // for performance reasons, separating this into temp table (in the next step it does a self join)
    create or replace temporary table economic_model_computed.cessionByLevelWithGross as
        select
            lpc_sc.scenarioid, 
            periodid,
            level,
            -- allow overriding level for retro by scenario
            -- scenario deals with levels (which are retrolevelype+1)
            -- note: due to rounding errors the sum can end up >100% which is incorrect and causes the ln func to throw an exception
            round(sum(lpc_sc.CessionGross),10) as Gross, 
            
            -- todo #debug-perf: add a "debug" boolean parameter to turn this on/off as it impacts performance heavily
            listagg(concat(' ', lpc_sc.retrocontractinvestorid, '(', trim(to_varchar(CessionGross * 100, '999.00')), '%)'), '\n') 
                within group (order by lpc_sc.retrocontractinvestorid asc) GrossExplanation
        from
            economic_model_computed.retroInvestorBlock_GrossPct lpc_sc
        group by 
            periodid, 
            level,
            lpc_sc.scenarioid;


    -- calculate how much we're ceding in each period to each level in each scenario. For tracing and debugging purposes making this a permanent table
    delete from 
        economic_model_computed.LevelBlock 
    where 
        -- delete data for scenarios we're about to compute
        scenarioid in (select scenarioid from economic_model_computed.scenariostocompute)
        -- as well as data for scenarios that no longer exist
        or scenarioid not in (select scenarioid from economic_model_scenario.scenario where isactive = 1);
        
    insert into economic_model_computed.LevelBlock
        select 
            g1.scenarioid, g1.periodid, g1.level, g1.gross, 
            // because snowflake does not have a "product" aggregate function (like "sum" but multiplying instead of adding), we
            // have to use exp(sum(ln())) combnination to achieve the same functionality.
            coalesce(case when max(g2.gross) = 1 then 0 else exp(sum(ln(case g2.gross when 1 then null else 1-g2.gross end))) end , 1)as Available, 

            -- todo #debug-perf: add a "debug" boolean parameter to turn this on/off as it impacts performance heavily
            listagg(concat('[', g2.level, ']: ', trim(to_varchar(g2.gross * 100, '999.00')), '%', ' {\n', g2.GrossExplanation,'\n}'), '\n') within group (order by g2.level asc) as AvailableExplanation
        from 
            economic_model_computed.cessionByLevelWithGross g1
            left join economic_model_computed.cessionByLevelWithGross g2 on g1.scenarioid = g2.scenarioid and g1.periodid = g2.periodid and g1.level > g2.level
        group by 
            g1.scenarioid, g1.periodid, g1.level, g1.gross;


    -- 4. For each block, gather all required information to calculate ceded premium, losses, rb & rp for base scenario and selected scenario    
    delete from 
        economic_model_computed.subjectBlockFactors 
    where 
        -- delete data for scenarios we're about to compute
        scenarioid in (select scenarioid from economic_model_computed.scenariostocompute)
        -- as well as data for scenarios that no longer exist
        or scenarioid not in (select scenarioid from economic_model_scenario.scenario where isactive = 1);


    // recalculate block factors for selected scenarios, but reuse previously stored block factors for base scenarios
    insert into economic_model_computed.subjectBlockFactors (retroblockid, scenarioid, difffactorshare, difffactorpremium)
        select 
            rb.retroblockId,
            sc.scenarioid,
            pls.sharefactor * coalesce(lb.available, 1) as diffFactorShare,
            diffFactorShare * pls.premiumfactor as diffFactorPremium
        from 
            economic_model_staging.retrotag rb
            inner join economic_model_staging.portlayerperiod lp on rb.periodid = lp.periodid
            
            // calculate blocks for all affected scenarios
            cross join economic_model_computed.ScenariosToCompute sc

            // need to find out how much we have available at specified level (level varies by scenario)
            inner join economic_model_staging.RetroConfiguration rpc on rb.RetroConfigurationid = rpc.RetroConfigurationid
            inner join economic_model_scenario.retrocontract_scenario rp_sc on rpc.retrocontractid = rp_sc.retrocontractid and rp_sc.scenarioid = sc.scenarioid
            -- note: left join because we might not have anything at lower levels
            left join economic_model_computed.LevelBlock lb on lb.periodid = rb.periodid and lb.level = rp_sc.level and lb.scenarioid = sc.scenarioid
            
            // find out the sharefactor and premium factor
            inner join economic_model_scenario.portlayer_scenario pls on lp.portlayerid = pls.portlayerid and pls.scenarioid = sc.scenarioid
        where 
            rp_sc.isactive = 1
            -- note: not excluding retros from analysis here, I will only apply this to subjectylt blocks (product development)
            -- as subject blocks themselves do not look like they will substantially impact powerbi import performance.
            // and rp_sc.includeinanalysis = 1*/
            ;

    delete from 
        economic_model_computed.subjectblock 
    where 
        -- delete data for scenarios we're about to compute
        scenarioid in (select scenarioid from economic_model_computed.scenariostocompute)
        -- as well as data for scenarios that no longer exist
        or scenarioid not in (select scenarioid from economic_model_scenario.scenario where isactive = 1);

    insert into economic_model_computed.subjectblock
        with
            affectedretroblockids as (
                select distinct retroblockid from economic_model_computed.subjectBlockFactors
            )
            , subjectBlockFactorDiffs as (
                select 
                    rbi.retroblockId,
                    s.scenarioid,
                    zeroifnull(sd.diffFactorShare) - zeroifnull(b.diffFactorShare) shareDiffFactor,
                    zeroifnull(sd.diffFactorPremium) - zeroifnull(b.diffFactorPremium) premiumDiffFactor
                from 
                    economic_model_computed.scenariostocompute s
                    cross join affectedretroblockids rbi
                    left outer join economic_model_computed.subjectBlockFactors sd on sd.scenarioid = s.scenarioid and sd.retroblockid = rbi.retroblockid
                    left outer join economic_model_computed.subjectBlockFactors b on b.retroblockid = rbi.retroblockid and b.scenarioid = s.parentscenarioid
                where
                    // filtering early because it improves performance due to reduced subsequent joins and math operations
                    (round(abs(sharedifffactor), 12) > 0 or round(abs(premiumdifffactor), 12) > 0)                    
            )
            select 
                f.scenarioid, 
                f.retroblockId,
                // note: round all currency amounts to integer to save space in both snowflake and in PowerBI
                // I think this should be safe as the errors it introduce will tend to even out on aggregate and on aggregate we're interested in millions, not dollars.
                // That said, perhaps this is best reserved for the fact tables used only from powerbi.
                round(pl.limit100pct   * pl.share * t.placement * s.sidesign                            * f.sharedifffactor, 0) as exposedlimit,
                round(pl.premium100pct * pl.share * t.placement * s.sidesign                            * f.premiumdifffactor, 0) as ExposedRP,
                round(pl.premium100pct * pl.share * t.placement * s.sidesign * per.shareoflayerduration * f.premiumdifffactor, 0) as PremiumProRata,
                round(PremiumProRata * pl.expenses, 0) as Expenses,
            from
                subjectBlockFactorDiffs f
                inner join economic_model_staging.retrotag t on f.retroblockid = t.retroblockid
                inner join economic_model_staging.portlayerperiod per on t.periodid = per.periodid
                inner join economic_model_staging.portlayer pl on per.portlayerid = pl.portlayerid
                inner join economic_model_staging.submission s on pl.submissionid = s.submissionid;

                
    delete from 
        economic_model_computed.investorBlockFactors 
    where 
        -- delete data for scenarios we're about to compute
        scenarioid in (select scenarioid from economic_model_computed.scenariostocompute)
        -- as well as data for scenarios that no longer exist
        or scenarioid not in (select scenarioid from economic_model_scenario.scenario where isactive = 1);
        
    // we use this table multiple times (self join) to determine diff, so making it a temp table instead of cte
    insert into economic_model_computed.investorBlockFactors
        select 
            bf.retroblockId,
            rib.retrocontractinvestorid,
            bf.scenarioid,
            bf.diffFactorShare * rib.cessiongross as diffFactorShare,
            bf.diffFactorPremium * rib.cessiongross as diffFactorPremium
        from 
            economic_model_computed.subjectBlockFactors bf
            left join economic_model_computed.retroInvestorBlock_GrossPct rib on bf.retroblockid = rib.retroblockid and bf.scenarioid = rib.scenarioid;

    delete from 
        economic_model_computed.cededblock
    where 
        -- delete data for scenarios we're about to compute
        scenarioid in (select scenarioid from economic_model_computed.scenariostocompute)
        -- as well as data for scenarios that no longer exist
        or scenarioid not in (select scenarioid from economic_model_scenario.scenario where isactive = 1);
        ;
    insert into economic_model_computed.cededblock
        with
            affectedretroblockinvestors as (
                select distinct retroblockid, retrocontractinvestorid from economic_model_computed.investorBlockFactors
            )
            , investorBlockFactorDiffs as (
                select 
                    rbi.retroblockId,
                    rbi.retrocontractinvestorid,
                    s.scenarioid,
                    zeroifnull(sd.diffFactorShare) - zeroifnull(b.diffFactorShare) shareDiffFactor,
                    zeroifnull(sd.diffFactorPremium) - zeroifnull(b.diffFactorPremium) premiumDiffFactor,
                from 
                    economic_model_computed.scenariostocompute s
                    cross join affectedretroblockinvestors rbi
                    left outer join economic_model_computed.investorBlockFactors sd on sd.scenarioid = s.scenarioid and sd.retroblockid = rbi.retroblockid and sd.retrocontractinvestorid = rbi.retrocontractinvestorid
                    left outer join economic_model_computed.investorBlockFactors b on b.retroblockid = rbi.retroblockid and b.scenarioid = s.parentscenarioid and b.retrocontractinvestorid = rbi.retrocontractinvestorid
                where
                    // filtering early because it improves performance due to reduced subsequent joins and math operations
                    (round(abs(sharedifffactor), 12) > 0 or round(abs(premiumdifffactor), 12) > 0)
            )
            select 
                scenarioid, 
                t.retroblockid, 
                retrocontractinvestorid, 
                // note: rounding all currency amounts to integer to save space in both snowflake and in PowerBI.
                // I think this should be safe as the errors it introduce will a) tend to even out on aggregate and 
                // b) we're interested in millions, not dollars.If needed we can limit this rounding to just the YLT fact 
                // tables that we primarily use from powerbi.   
                // note #2: shareDiffFactor contains the scenario(factors)-base(factors), where factors=available*cessiongross*sharefactor
                // while for premiumDiffFactor factors = available*cessiongross*sharefactor*premiumfactor. These parameters vary by scenario
                // so they are the ones that cause a diff between two scenarios.
                round(limit100pct * share * sidesign * shareDiffFactor, 0) as exposedlimit,
                round(premium100pct * share * sidesign * premiumDiffFactor                           , 0) as ExposedRP,
                round(premium100pct * share * sidesign * premiumDiffFactor * per.shareoflayerduration, 0) as PremiumProRata,
                round(PremiumProRata * pl.expenses, 0) as ExpensesProRata,
            from 
                investorBlockFactorDiffs f
                inner join economic_model_staging.retrotag t on f.retroblockid = t.retroblockid
                inner join economic_model_staging.portlayerperiod per on t.periodid = per.periodid
                inner join economic_model_staging.portlayer pl on per.portlayerid = pl.portlayerid
                inner join economic_model_staging.submission s on pl.submissionid = s.submissionid;

                
    -- adding gross blocks as well though
    delete from 
        economic_model_computed.grossblock
    where 
        -- delete data for scenarios we're about to compute
        scenarioid in (select scenarioid from economic_model_computed.scenariostocompute)
        -- as well as data for scenarios that no longer exist
        or scenarioid not in (select scenarioid from economic_model_scenario.scenario where isactive = 1);
        ;    
    insert into economic_model_computed.grossblock (scenarioid, portlayerid, exposedlimit, exposedrp, premium, expenses)
        with factors as
        (
            select 
                s.scenarioid, 
                portlayerid, 
                sharefactor, 
                premiumfactor * sharefactor as premiumShareFactor,
                s.parentscenarioid
            from 
                economic_model_scenario.portlayer_scenario pl
                inner join economic_model_computed.scenariostocompute s on pl.scenarioid = s.scenarioid
        )
        , affectedPortLayers as (
            select distinct portlayerid from factors
        )
        , diffFactor as (
            select
                s.scenarioid, 
                ap.portlayerid,
                zeroifnull(f.sharefactor) - zeroifnull(fb.sharefactor) as sharefactor_diff,
                zeroifnull(f.premiumShareFactor) - zeroifnull(fb.premiumShareFactor) as premiumShareFactor_diff
            from
                economic_model_computed.scenariostocompute s
                cross join affectedPortLayers ap
                left join factors f on f.portlayerid = ap.portlayerid and f.scenarioid = s.scenarioid
                left join factors fb on fb.portlayerid = ap.portlayerid and zeroifnull(s.parentscenarioid) = zeroifnull(fb.scenarioid)
        )
        select 
            f.scenarioid, 
            pl.portlayerid,
            // note: round all currency amounts to integer to save space in both snowflake and in PowerBI
            // I think this should be safe as the errors it introduce will tend to even out on aggregate and on aggregate we're interested in millions, not dollars.
            // That said, perhaps this is best reserved for the fact tables used only from powerbi.
            round(pl.limit100pct   * pl.share * s.sidesign * f.sharefactor_diff, 0) as exposedlimit,
            round(pl.premium100pct * pl.share * s.sidesign * f.premiumShareFactor_diff, 0) as ExposedRP,
            round(pl.premium100pct * pl.share * s.sidesign * f.premiumShareFactor_diff, 0) as Premium,
            round(pl.premium100pct * pl.share * s.sidesign * f.premiumShareFactor_diff * pl.expenses, 0) as Expenses,
        from 
            diffFactor f
            inner join economic_model_staging.portlayer pl on pl.portlayerid = f.portlayerid
            inner join economic_model_staging.submission s on pl.submissionid = s.submissionid;


    -- adding gross blocks as well though
    delete from 
        economic_model_computed.grossblockusd
    where 
        -- delete data for scenarios we're about to compute
        scenarioid in (select scenarioid from economic_model_computed.scenariostocompute)
        -- as well as data for scenarios that no longer exist
        or scenarioid not in (select scenarioid from economic_model_scenario.scenario where isactive = 1);
        ;    
    insert into economic_model_computed.grossblockusd (scenarioid, portlayerid, exposedlimit, exposedrp, premium, expenses)
        select 
            sc.scenarioid, 
            portfolioid, 
            exposedlimit * fx.rate as exposedlimit, 
            exposedrp * fx.rate as exposedrp, 
            b.premium * fx.rate as premium,
            b.expenses * fx.rate as expenses
        from 
            economic_model_computed.grossblock b
            inner join economic_model_scenario.scenario sc on b.scenarioid = sc.scenarioid
            inner join economic_model_staging.portlayer pl on b.portlayerid = pl.portlayerid
            inner join economic_model_staging.submission s on pl.submissionid = s.submissionid
            inner join economic_model_raw.fxrate fx on fx.basecurrency = 'USD' and fx.currency = s.currency and fx.fxdate = coalesce(sc.fxdate, s.fxdate)
        ;
    
end
$$;