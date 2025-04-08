CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.PROCESS_SCENARIO(SCENARIOID NUMBER)
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
$$
begin

    use schema economic_model_computed;

    // 1. Update capital calculations (required capital for each retro, signed % for each investor with a specified $ amount).
    // The calculated signed % can be overriden in the scenario editor or in revo, but will be used if the above two are not provided.
    call calculate_capital(:scenarioId);

    -- 2.a. prepare visible scenarios
    create or replace temporary table ScenarioFiltered as
        select 
            *
        from 
            economic_model_scenario.scenario 
        where
            isactive = 1 
            and 
            (
                -- scenarioid = 4439 or //temp: testing
                (scenarioid = :scenarioId OR :scenarioId is null) or
                -- must include scenario 1 for baseline
                scenarioid = 1
            )
        ;
        
    -- 2.b. resolve retro program scenario
    create or replace temporary table RetroProgramScenario as
    select 
        s.scenarioid, 
        rp.retroprogramid, 
        rp.isspecific,
        coalesce(rp_s.level-1, rp.retroleveltype) as retroleveltype,
        coalesce(rp_s.IsActive, rp.IsActive) as IsActive,
        (coalesce(rp_s.isactive <> rp.isactive, false) or coalesce(rp_s.level - 1 <> rp.retroleveltype, false)) as hasOverride
    from 
        economic_model_staging.retroprogram rp
        cross join scenarioFiltered s
        left join economic_model_scenario.retroprogram_override rp_s on rp.retroprogramid = rp_s.retroprogramid and s.scenarioid = rp_s.scenarioid
    order by 
        retroprogramid;

    -- 2.c. resolve retro investor scenarios
    create or replace temporary table RetroConfigurationInvestorScenario as
    select 
        s.scenarioid,
        rci.retroconfigurationid,
        rci.retroinvestorid,
        rci.retroconfigurationinvestorid,
        coalesce(rcio.investmentsigned, rci.investmentsigned) as investmentsigned
    from 
        economic_model_staging.retroconfigurationinvestor rci
        cross join scenarioFiltered s
        left join economic_model_scenario.retroconfigurationinvestor_override rcio on rcio.retroconfigurationinvestorid = rci.retroconfigurationinvestorid and s.scenarioid = rcio.scenarioid
    order by 
        retroinvestorid;

    -- 2.d. resolve portlayer scenarios
    create or replace temporary table PortLayerScenario as
    select 
        s.scenarioid,
        pl.portlayerid,
        pl.portfolioid,
        pl.submissionid,
        pl.inception,
        pl.expiration,
        pl.limit100pct,
        pl.premium100pct,
        pl.expenses,
        pl.share,
        coalesce(plo.sharefactor, pl.sharefactor) as sharefactor,
        coalesce(plo.premiumfactor, pl.premiumfactor) as premiumfactor,
        (coalesce(plo.sharefactor <> pl.sharefactor, false) or coalesce(plo.sharefactor <> pl.sharefactor, false)) as hasOverride
    from 
        economic_model_staging.portlayer pl
        cross join scenarioFiltered s
        left join economic_model_scenario.portlayer_override plo on pl.portlayerid = plo.portlayerid and s.scenarioid = plo.scenarioid
    ;
    
    -- 3.Calculate cession gross % for each block (retro and investor)
    create or replace temporary table retroInvestorBlock_GrossPct as
    select
        rci_o.scenarioid,
        rb.retroblockid,
        r.retroprogramid,
        rci.retroinvestorid,
        -- todo: I noticed that some retros with IsSpecific=1 have multiple investors for a given layer. Check to make sure if this is expected.
        -- note: attempt to use 1) overriden investmentsigned from the scenario editor if available, then 2) revo and finally 3) the calculated share
        rb.placement * case when r.isspecific =1 then ra.cessiongross else coalesce(rci_o.investmentsigned, rci.investmentsigned, rci_o.investmentsignedpctcalculated) end as CessionGross
    from 
        economic_model_staging.retroblock rb
        inner join economic_model_staging.RetroConfiguration rpc on rpc.RetroConfigurationid = rb.RetroConfigurationid
        inner join economic_model_staging.portlayerperiod lp on lp.periodid = rb.periodid
        inner join economic_model_staging.portlayer pl on pl.portlayerid = lp.portlayerid
        inner join economic_model_staging.retroconfigurationinvestor rci on rci.retroconfigurationid = rpc.retroconfigurationid
        inner join economic_model_staging.retroprogram r on rpc.retroprogramid = r.retroprogramid
        // left join because there will not be a retroallocation for projeted layers, this should only happen for retro with IsSpecific=1
        left join economic_model_staging.retroallocation ra on ra.layerid = pl.layerid and ra.retroinvestorid = rci.retroinvestorid
        // investor share varies by scenario
        left join economic_model_scenario.retroconfigurationinvestor_override rci_o on rci_o.RetroConfigurationInvestorId = rci.RetroConfigurationInvestorId
    where 
        rci_o.scenarioid in (select scenarioid from scenariofiltered) 
        -- we need the baseline so we can find the diff
        or rci_o.scenarioid = 1;


    -- 2.b. calculate gross cession per retro program. This will be used for PM dashboard, retro product development (subject & gross perspectives).
    create or replace temporary table retroBlock_GrossPct as
    select scenarioid, retroblockid, sum(cessiongross) as CessionGross from retroInvestorBlock_GrossPct 
    group by scenarioid, retroblockid;


    -- 3. For each period, calculate how much we're ceding to each LEVEL in each scenario
    create or replace table LevelBlock as
    -- calculate in each period, calculate how much we''re ceding to each level in each scenario
    with cessionByLevelWithGross as (
        select 
            lpc_sc.scenarioid, 
            periodid, 
            -- allow overriding retroleveltype for retro by scenario
            -- scenario deals with levels (which are retrolevelype+1)
            retroleveltype,
            -- we''re zeroing out inactive retros when looking at their contribution to lower level retros. Scenarios can de-/activate retros if the user wants.
            -- note: due to rounding errors the sum can end up >100% which is incorrect and causes the ln func to throw an exception
            round(sum(case when rps.IsActive then lpc_sc.CessionGross else 0 end),10) as Gross, 
            -- we''re zeroing out inactive retros when looking at their contribution to lower level retros. Scenarios can de-/activate retros if the user wants.
            listagg(
                case 
                    when rps.IsActive then concat(' ', rps.RetroProgramId, '(', trim(to_varchar(CessionGross * 100, '999.00')), '%)') 
                    else concat(' -- Ignoring inactive retro ', rps.retroProgramId) 
                end, 
                '\n') 
                within group (order by rps.IsActive desc, rps.RetroProgramId asc) GrossExplanation
        from
            retroblock_grosspct lpc_sc
            inner join economic_model_staging.retroblock lpc on lpc.retroblockId = lpc_sc.retroblockId
            inner join economic_model_staging.RetroConfiguration rp_cfg on rp_cfg.RetroConfigurationid = lpc.RetroConfigurationid
            left join retroprogramscenario rps on rp_cfg.retroprogramid = rps.retroprogramid and lpc_sc.scenarioid = rps.scenarioid
        group by 
            lpc_sc.scenarioid, periodid, retroleveltype
    )
    select 
        g1.scenarioid, g1.periodid, g1.retroleveltype, g1.gross, 
        // because snowflake does not have a product aggregate function (like sum but multiplying instead of adding), we
        // have to use exp(sum(ln())) combnination to achieve the same functionality.
        coalesce(case when max(g2.gross) = 1 then 0 else exp(sum(ln(case g2.gross when 1 then null else 1-g2.gross end))) end , 1)as Available, 
        listagg(concat('[', g2.RetroLevelType, ']{\n', g2.GrossExplanation,'\n}'), '\n') within group (order by g2.RetroLevelType asc) as AvailableExplanation
    from 
        cessionByLevelWithGross g1
        left join cessionByLevelWithGross g2 on g1.scenarioid = g2.scenarioid and g1.periodid = g2.periodid and g1.retroleveltype > g2.retroleveltype
    group by 
        g1.scenarioid, g1.periodid, g1.retroleveltype, g1.gross
    order by 
        g1.scenarioid, g1.periodid, g1.retroleveltype;
        
    //////////////////////

    -- 4. For each block, calculate how much premium and exposure we're ceding to each retro investor (currency amounts).
    -- Values for non-base scenarios are a diff compared to the base scenario. To get the result for a scenario, sum with base.
   
    -- 4.a find all required base+scenario percentages for each scenario/portlayer/period/investor
    create or replace temporary table retroInvestorBlockInfo as
        select 
            sc.scenarioid,
            pl_base.portlayerid,
            rpc.retroprogramid,
            rib.retroinvestorid,
            lp.yeltperiodid,
            lp.periodid,
            pl_base.limit100pct,
            pl_base.share,
            pl_base.premium100Pct,
            s.sidesign,
            s.currency,
            lp.shareoflayerduration,

            -- important note: 
            -- placement is already included in retroInvestorBlock_GrossPct.CessionGross
            -- but we need the placement itself when calculating the subject values.
            rb.placement,
    
            lb.available,
            lb_base.available availableBase,
            
            rib.cessiongross,
            rib_base.cessiongross cessiongrossBase,
            
            pls.sharefactor,
            pl_base.sharefactor sharefactorBase,
            
            pls.premiumfactor,
            pl_base.premiumfactor premiumfactorBase
        from 
            economic_model_staging.retroblock rb
            inner join economic_model_staging.portlayerperiod lp on rb.periodid = lp.periodid
            
            // calculate blocks for all selected scenarios
            cross join scenariofiltered sc
    
            // find out which level the retro in each block is at (in the given scenario)
            inner join economic_model_staging.RetroConfiguration rpc on rb.RetroConfigurationid = rpc.RetroConfigurationid
            inner join retroprogramscenario rp_sc on rpc.retroprogramid = rp_sc.retroprogramid and rp_sc.scenarioid = sc.scenarioid
    
            // find out how much is available at given level in given scenario (and base scenario)
            inner join LevelBlock lb on lb.retroleveltype = rp_sc.retroleveltype and lb.periodid = rb.periodid and lb.scenarioid = sc.scenarioid
            inner join LevelBlock lb_base on lb_base.retroleveltype = rp_sc.retroleveltype and lb_base.periodid = lp.periodid and lb_base.scenarioid = 1
            
            // find out the sharefactor and premium factor
            inner join portlayerscenario pls on lp.portlayerid = pls.portlayerid and pls.scenarioid = sc.scenarioid
            inner join economic_model_staging.portlayer pl_base on pl_base.portlayerid = lp.portlayerid
    
            // find out side sign
            inner join economic_model_staging.submission s on pl_base.submissionid = s.submissionid
    
            // find out how much each investor has in scenario
            inner join retroInvestorBlock_GrossPct rib on rb.retroblockid = rib.retroblockid and sc.scenarioid = rib.scenarioid
            inner join retroInvestorBlock_GrossPct rib_base on rib_base.retroblockid = rib.retroblockid and rib_base.retroinvestorid = rib.retroinvestorId and rib_base.scenarioid = 1
    ;
    
    -- 4.b. calculate currency amount for each retro block (all three perspectives)
    delete from RetroBlock
    where scenarioid in (select scenarioid from scenarioFiltered);
  
    insert into RetroBlock
    with cte as(
        select 
            scenarioid,
            portlayerid,
            retroprogramid,
            yeltperiodid,
            periodid,
            limit100pct,
            share,
            premium100Pct,
            sidesign,
            shareoflayerduration,
            placement,
            available,
            availableBase,
            sharefactor,
            sharefactorBase,
            premiumfactor,
            premiumfactorBase,
            sum(cessiongross) cessiongross,
            sum(cessiongrossBase) cessiongrossbase,
            currency
        from 
            retroinvestorblockinfo
        group by
            // the only thing we aren't grouping by is the retroinvestor
            scenarioid,
            portlayerid,
            retroprogramid,
            yeltperiodid,
            periodid,
            limit100pct,
            share,
            premium100Pct,
            sidesign,
            currency,
            shareoflayerduration,
            placement,
            available,
            availableBase,
            sharefactor,
            sharefactorBase,       
            premiumfactor,
            premiumfactorBase
    )
    , cte2 as (
        select 
            scenarioid, 
            portlayerid, 
            retroprogramid, 
            yeltperiodid,
            periodid,
            currency,
            // note: round all currency amounts to integer to save space in both snowflake and in PowerBI
            // I think this should be safe as the errors it introduce will tend to even out on aggregate and on aggregate we're interested in millions, not dollars.
            // That said, perhaps this is best done in the preagregated fact tables for consumption from powerbi.
            
            // Ceded
            // note: cessiongross alredy includes placement, so we don't multiply by placement here
            round(limit100pct * share *  sidesign 
                // we take scenario 1 fully and only the diff for other scenarios
                * case 
                    when scenarioid = 1 then available * cessiongross * sharefactor 
                    else available * cessiongross * sharefactor - availablebase * cessiongrossbase * sharefactorbase
                end
            , 0) as exposedlimit_ceded,
            round(premium100pct * share * sidesign 
                // we take scenario 1 fully and only the diff for other scenarios
                * case 
                    when scenarioid = 1 then available * cessiongross * sharefactor * premiumFactor
                    else available * cessiongross * sharefactor * premiumFactor - availablebase * cessiongrossbase * sharefactorbase * premiumFactorBase
                end
            , 0) as ExposedRP_ceded,
            round(premium100pct * share * sidesign * shareoflayerduration 
                // we take scenario 1 fully and only the diff for other scenarios
                * case 
                    when scenarioid = 1 then available * cessiongross * sharefactor * premiumFactor
                    else available * cessiongross * sharefactor * premiumFactor - availablebase * cessiongrossbase * sharefactorbase * premiumFactorBase
                end
            , 0) as PremiumProRata_ceded,
            
            // Subject
            round(limit100pct * share * placement * sidesign 
                // we take scenario 1 fully and only the diff for other scenarios
                * case 
                    when scenarioid = 1 then available * sharefactor 
                    else available * sharefactor - availablebase * sharefactorbase
                end
            , 0) as exposedlimit_subject,
            round(premium100pct * share * placement * sidesign 
                // we take scenario 1 fully and only the diff for other scenarios
                * case 
                    when scenarioid = 1 then available * sharefactor * premiumFactor
                    else available * sharefactor * premiumFactor - availablebase * sharefactorbase * premiumFactorBase
                end
            , 0) as ExposedRP_subject,
            round(premium100pct * share * placement * sidesign * shareoflayerduration 
                // we take scenario 1 fully and only the diff for other scenarios
                * case 
                    when scenarioid = 1 then available * sharefactor * premiumFactor
                    else available * sharefactor * premiumFactor - availablebase * sharefactorbase * premiumFactorBase
                end
            , 0) as PremiumProRata_subject,
            
            // Gross
            round(limit100pct * share * sidesign 
                // we take scenario 1 fully and only the diff for other scenarios
                * case 
                    when scenarioid = 1 then sharefactor 
                    else sharefactor - sharefactorbase
                end
            , 0) as exposedlimit_gross,
            round(premium100pct * share * sidesign 
                // we take scenario 1 fully and only the diff for other scenarios
                * case 
                    when scenarioid = 1 then sharefactor * premiumFactor
                    else sharefactor * premiumFactor - sharefactorbase * premiumFactorBase
                end
            , 0) as ExposedRP_gross,
            round(premium100pct * share * sidesign * shareoflayerduration 
                // we take scenario 1 fully and only the diff for other scenarios
                * case 
                    when scenarioid = 1 then available * sharefactor * premiumFactor
                    else sharefactor * premiumFactor - sharefactorbase * premiumFactorBase
                end
            , 0) as PremiumProRata_gross
        from 
            cte
    )
    select 
        c.*, per.periodstart, per.periodend
    from 
        cte2 c
        inner join economic_model_staging.PORTLAYERPERIOD per on c.periodid = per.periodid
    where 
        // only include rows that contribute
        // results to eliminate empty diff rows 
        // (~70% size reduction as not all layers
        // and periods are impacted by all scenarios)
        PremiumProRata_gross <> 0
        or ExposedRP_gross <> 0
        or exposedlimit_gross <> 0

        or PremiumProRata_subject <> 0
        or ExposedRP_subject <> 0
        or exposedlimit_subject <> 0

        or PremiumProRata_ceded <> 0
        or ExposedRP_ceded <> 0
        or exposedlimit_ceded <> 0;


    -- 4.c. Calculate currency amounts for each retro investor block (ceded perspective only)
    delete from RetroInvestorBlock
    where scenarioid in (select scenarioid from scenarioFiltered);
   
    insert into RetroInvestorBlock
    with cte as(
        select 
            scenarioid, 
            portlayerid, 
            retroinvestorid, 
            yeltperiodid,
            periodid,
            currency,
            
            // note: rounding all currency amounts to integer to save space in both snowflake and in PowerBI.
            // I think this should be safe as the errors it introduce will a) tend to even out on aggregate and 
            // b) we're interested in millions, not dollars.If needed we can limit this rounding to just the YLT fact 
            // tables that we primarily use from powerbi.
            
            // Ceded
            round(limit100pct * share * sidesign 
                // we take scenario 1 fully and only the diff for other scenarios
                * case 
                    when scenarioid = 1 then available * cessiongross * sharefactor 
                    else available * cessiongross * sharefactor - availablebase * cessiongrossbase * sharefactorbase
                end
            , 0) as exposedlimit_ceded,
            round(premium100pct * share * sidesign 
                // we take scenario 1 fully and only the diff for other scenarios
                * case 
                    when scenarioid = 1 then available * cessiongross * sharefactor * premiumFactor
                    else available * cessiongross * sharefactor * premiumFactor - availablebase * cessiongrossbase * sharefactorbase * premiumFactorBase
                end
            , 0) as ExposedRP_ceded,
            round(premium100pct * share * sidesign * shareoflayerduration 
                // we take scenario 1 fully and only the diff for other scenarios
                * case 
                    when scenarioid = 1 then available * cessiongross * sharefactor * premiumFactor
                    else available * cessiongross * sharefactor * premiumFactor - availablebase * cessiongrossbase * sharefactorbase * premiumFactorBase
                end
            , 0) as PremiumProRata_ceded,
        from 
            retroinvestorblockinfo
    )
    select 
        c.*, per.periodstart, per.periodend
    from 
        cte c
        inner join economic_model_staging.portlayerperiod per on c.periodid = per.periodid
    where 
        // only include rows that contribute
        // results to eliminate empty diff rows 
        // (~70% size reduction as not all layers
        // and periods are impacted by all scenarios)
        PremiumProRata_ceded <> 0
        or ExposedRP_ceded <> 0
        or exposedlimit_ceded <> 0;

    -- 5. Precalculate block YLT tables at both levels (retro & investor)
    // todo: consider moving the below tables to a separate schema. Possibly each into its own
    // schema, e.g. economic_model_retroproductdevelopment, economic_model_pmdashboard...

    -- -- 5.a. retro-level
    -- delete from RetroBlockYLT where scenarioid in (select scenarioid from scenarioFiltered);
    -- insert into RetroBlockYLT 
    --     select 
    --         y.year, 
    --         y.peril, 
    --         y.lossviewgroup, 
    --         pl.portlayerid, 
    --         rb.retroprogramid, 
    --         rb.scenarioid,
        
    --         per.periodstart,
    --         per.periodend,

    --         // note: this table is used from powerbi for retro product development. We only need
    --         // the subject perspective for this.
    --         /*
    --         round(exposedlimit_gross * totalloss)  grossLoss,
    --         round(exposedrp_gross * totalrp)  grossRP,
    --         round(exposedrp_gross * totalrb)  grossRB,
    --         */
        
    --         round(exposedlimit_subject * totalloss)  subjectLoss,
    --         round(exposedrp_subject * totalrp)  subjectRP,
    --         round(exposedrp_subject * totalrb)  subjectRB,

    --         // note: see comment above gross columns
    --         /*
    --         round(exposedlimit_ceded * totalloss)  cededLoss,
    --         round(exposedrp_ceded * totalrp)  cededRP,
    --         round(exposedrp_ceded * totalrb)  cededRB
    --         */
    --     from 
    --         retroblock rb
    --         inner join economic_model_staging.yelpt y on rb.yeltperiodid = y.yeltperiodid
    --         inner join economic_model_staging.portlayerperiod per on rb.periodid = per.periodid
    --         inner join economic_model_staging.portlayer pl on rb.portlayerid = pl.portlayerid
    --         // only calculate for slected scenarios
    --         inner join scenariofiltered s on rb.scenarioid = s.scenarioid
    --         // and only for selected retros
    --         inner join economic_model_scenario.includedretroprogram irp on irp.scenarioid = rb.scenarioid and irp.retroprogramid = rb.retroprogramid
    -- ;
    
end
$$;