CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.PROCESS_MODEL(SCENARIOID NUMBER)
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
declare
    currLevel int;
    lvlCurr cursor for select $1 level from values (1), (2), (3), (4), (5), (10) order by level asc;
begin

    -- 1. prepare

    -- resolve scenario data
    call economic_model_scenario.resolve_scenario_data();
    
    --  prepare temp table with visible scenarios
    create or replace temporary table economic_model_computed.ScenarioFiltered as
        select
            *
        from 
            economic_model_scenario.scenario 
        where
            isactive = 1
            and (scenarioid = :scenarioid or :scenarioid is null)
        ;

    -- clear any old data we're about to recalculate
    call economic_model_computed.clearscenariodatafromtable('cededblock', :scenarioId);
    call economic_model_computed.clearscenariodatafromtable('subjectblock', :scenarioId);
    call economic_model_computed.clearscenariodatafromtable('grossblock', :scenarioId);
    call economic_model_computed.clearscenariodatafromtable('contractresult', :scenarioId);
    call economic_model_computed.clearscenariodatafromtable('investorresult', :scenarioId);

    -- prepare information about retrocontracts for use by the calculatemetrics procedure
    truncate economic_model_computed.calculationcontract;
    insert into economic_model_computed.calculationcontract
        with targetCollateralUsed as (
            select retrocontractid, scenarioid, coalesce(max_by(targetcollateraloverride, startdate), max_by(targetcollateralrevo, startdate)) collateral from economic_model_scenario.retroconfiguration_scenario rc 
            group by retrocontractid, scenarioid
        )
        select 
            r.scenarioid, 
            r.retrocontractid, 
            1 /*1=retrocontract*/, 
            climateload, 
            nonmodeledload, 
            reinsurancebrokerageonnetpremium, 
            commission, 
            profitcommission, 
            reinsuranceexpensesoncededcapital, 
            reinsuranceexpensesoncededpremium, 
            capitalcalculationlossview, 
            capitalcalculationtargetreturnperiod,
            1 /*capital calculation method, currently only tvar supported so this is a placeholder parameter*/,
            collateral,
            exposurestart,
            exposureend,
            case when retroprogramtype = 2 then 'RAD' else 'LOD' end
        from 
            economic_model_computed.ScenarioFiltered sc
            inner join economic_model_scenario.retrocontract_scenario r on r.scenarioid = sc.scenarioid
            inner join targetCollateralUsed tc on tc.scenarioid = r.scenarioid and tc.retrocontractid = r.retrocontractid
        where 
            r.scenarioid is not null
         ;

    -- Prepare base blocks. These do not include cessiogross % nor available % (not scenario and level dependent). 
    -- They are used to base subsequent calculations on. (available for subject, available + cessiongross broken down by investor for ceded)
    -- We've already done the filtering and pulled in the relevant factors (except the ones we don't yet know)
    create or replace temporary table economic_model_computed.baseblockinfo as 
        select
            pls.scenarioid,
            per.periodid,
            pls.portlayerid,
            pls.layerid,
            b.retroblockid,
            rc.retrocontractid,
            rc.retroconfigurationid,
            rps.level,
            rps.inception,
            rps.isspecific,

            // calculation factors for premium, expensesAmt, and exposed limit/premium
            pls.limit100pct,
            pls.premium100pct,
            pls.share,
            pls.sharefactor,
            b.placement,
            pls.premiumfactor,
            per.shareoflayerduration,
            pls.expenses,

            // todo: add side sign
            s.sidesign,

            pls.reinstcount,

            // calculated values
            s.sidesign * pls.limit100pct * pls.share * pls.sharefactor * b.placement as exposedLimit,
            s.sidesign * pls.premium100pct * pls.share * pls.sharefactor * pls.premiumfactor * b.placement as exposedPremium,
            s.sidesign * pls.premium100pct * pls.share * pls.sharefactor * pls.premiumfactor * b.placement as exposedExpenses,
            s.sidesign * pls.premium100pct * pls.share * pls.sharefactor * pls.premiumfactor * b.placement * per.shareoflayerduration as proRataPremium,
            s.sidesign * pls.premium100pct * pls.share * pls.sharefactor * pls.premiumfactor * b.placement * per.shareoflayerduration * pls.expenses as proRataPremiumExpenses,

            // we'll need non placed blocks for calculating cession (cession gross % includes placement)
            s.sidesign * pls.limit100pct * pls.share * pls.sharefactor as nonplaced_exposedLimit,
            s.sidesign * pls.premium100pct * pls.share * pls.sharefactor * pls.premiumfactor as nonplaced_exposedPremium,
            s.sidesign * pls.premium100pct * pls.share * pls.sharefactor * pls.premiumfactor as nonplaced_exposedExpenses,
            s.sidesign * pls.premium100pct * pls.share * pls.sharefactor * pls.premiumfactor * per.shareoflayerduration as nonplaced_proRataPremium,
            s.sidesign * pls.premium100pct * pls.share * pls.sharefactor * pls.premiumfactor * per.shareoflayerduration * pls.expenses as nonplaced_proRataPremiumExpenses

            // calculation (might need 4 more columns here: premium+expenses/placed+nonplaced for seasonal calc)
        from 
            economic_model_staging.retrotag b
            // cross join so we get blocks with "IsSpecific" for all scenarios (filtered)
            cross join economic_model_computed.scenariofiltered sf
            inner join economic_model_staging.portlayerperiod per on b.periodid = per.periodid
            inner join economic_model_scenario.portLayer_scenario pls on per.portlayerid = pls.portlayerid and pls.scenarioid = sf.scenarioid
            inner join economic_model_staging.submission s on s.submissionid = pls.submissionid
            inner join economic_model_staging.retroconfiguration rc on b.retroconfigurationid = rc.retroconfigurationid
            inner join economic_model_scenario.retrocontract_scenario rps on rps.scenarioid = sf.scenarioid and rps.retrocontractid = rc.retrocontractid
        where 
            rps.isactive = 1;

    create or replace temporary table investedAmountByRetroContract as
        with investedAmountByRetroConfig as (
            select 
                rls.scenarioid, rc.retrocontractid, rc.retroconfigurationid, rc.startdate, sum(rls.investmentsignedamt) totalInvestedAmount
            from 
                economic_model_staging.retroconfiguration rc
                cross join economic_model_computed.scenariofiltered sf
                inner join economic_model_scenario.retroinvestmentleg_scenario rls on rls.retroconfigurationid = rc.retroconfigurationid and rls.scenarioid = sf.scenarioid
            group by 
                rls.scenarioid, rc.retrocontractid, rc.retroconfigurationid, rc.startdate
        )
        select 
            scenarioid, 
            retrocontractid,
            max_by(totalInvestedAmount, startdate) totalInvestedAmount
        from 
            investedamountbyretroconfig
            group by scenarioid, retrocontractid;

            
    -- 2. let's start generating and saving blocks

-- todo: we need a unique id for ceded blocks composed of retroblock and investorid (check if this ever leads to duplicates... it should not)
   
    -- 2.1. Retros with isspecific flag are not scenario-dependent and they don't interact with other retros, so we can calculate their ceded blocks right away.
    -- the cessiongross comes from the retroallocation table, and avilable is always 1 (todo: confirm with PC). 
    -- Todo: we can put cessiongross in RetroTag in the staging schema, no need to have the retroallocation in the staging schema.
    --      actually, we'd need a InvestorTag for that since RetroAllocations are at the investor granularity.
    -- Todo: It looks like some retros with IsSpecific=1 have multiple investors for a given layer. Is this expected? Investigate.
     insert into economic_model_computed.cededblock(
            scenarioid, retroblockid, retrocontractinvestorid, cessiongross, exposedlimit, exposedrp, exposedexpenses, premiumprorata, expensesprorata, reinstcount,
            diag_limit100pct, diag_premium100pct, diag_share, diag_sharefactor, diag_placement, diag_premiumfactor, diag_shareoflayerduration, diag_expenses, diag_available, diag_available_explanation, diag_sidesign)
        select
            b.scenarioid, 
            b.retroblockid,
            rci.retrocontractinvestorid,
            -- todo: It looks like some retros with IsSpecific=1 have multiple investors for a given layer. Is this expected? Investigate.
            b.placement * ra.cessiongross as CessionGross,
            b.nonplaced_exposedlimit * cessiongross as exposedlimit,
            b.nonplaced_exposedpremium * cessiongross as exposedpremium,
            b.nonplaced_exposedexpenses * cessiongross as exposedexpenses,
            b.nonplaced_proratapremium * cessiongross as premiumprorata,
            b.nonplaced_proratapremiumexpenses * cessiongross as expensesprorata,
            reinstcount,
            limit100pct, premium100pct, share, sharefactor, placement, premiumfactor, shareoflayerduration, expenses, 1, 'Specific retros always have Available = 1',
            b.sidesign
        from 
            economic_model_computed.baseblockinfo b
            inner join economic_model_scenario.retroinvestmentleg_scenario rci on rci.retroconfigurationid = b.retroconfigurationid and rci.scenarioid = b.scenarioid
            inner join economic_model_staging.retroallocation ra on ra.layerid = b.layerid and ra.retrocontractinvestorid = rci.retrocontractinvestorid
        where
            b.isspecific = 1;

       
    -- 2.2. For non-specific retros, we have to go one level at a time, becase the calculated capital for one retro can impact retros in subsequent levels.
    FOR lvlRow IN lvlCurr DO

        -- todo: check lvl 3 retros (retroleveltype=2), I think they should have -sign. Ask PC to verify if the same calculation applies to them.
        currLevel := lvlRow.level;

        -- 2.2.1. start by calculating the subject blocks for the current level
        create or replace temporary table economic_model_computed.subjectBlockInfo as 
            with
                -- convenience, I use the variable in two places, so when I do manual testing for a level, I can change it here in one place
                currentLevel as (
                    select 
                        :currLevel 
                        as currLevel
                )
                , grossByLevelUnderRetroAndPeriod as (
                    select
                        sbi.scenarioid, 
                        sbi.periodid,
                        sbi.retrocontractid,
                        sbi.level as subjectLevel,
                        r.level as lowerLevel,
                        sum(cessiongross) as cessiongross,
                        listagg(concat(' ', cb.retrocontractinvestorid, '(', trim(to_varchar(CessionGross * 100, '999.00')), '%)'), '\n') within group (order by cb.retrocontractinvestorid asc) CessionGrossExplanation
                    from 
                        -- start with subject blocks we're working on
                        economic_model_computed.baseblockinfo sbi
                        -- for each subject block, find all other tags in the same period
                        inner join economic_model_staging.retrotag t on t.periodid = sbi.periodid
                        -- ensure that the level of those retrotags is less than the current one
                        inner join economic_model_staging.retroconfiguration rc on rc.retroconfigurationid = t.retroconfigurationid
                        inner join economic_model_scenario.retrocontract_scenario r on r.retrocontractid = rc.retrocontractid and r.scenarioid = sbi.scenarioid and r.level < sbi.level
                            -- optionally we can ensure that the start date is <= to the inception of the subject block's retro
                            -- note: currently commenting this out as it's not yet decided if this will be the desired behavior
                            -- and r.inception <= sbi.inception
                        -- now get the actual ceded blocks. to summarize, they are blocks in the same scenario and period, at a lower level, and (optionally) whose retros have not started after the one we're looking at.
                        inner join economic_model_computed.cededblock cb on cb.scenarioid = sbi.scenarioid and cb.retroblockid = t.retroblockid
                        cross join currentLevel
                    where
                        sbi.level = currLevel
                    group by 
                        sbi.scenarioid, 
                        sbi.periodid,
                        sbi.retrocontractid,
                        subjectLevel,
                        lowerLevel
                ),
                availability as (
                    select 
                        scenarioid, 
                        periodid,
                        retrocontractid,
                        subjectLevel,
                        -- if a lower level grabbed 100%, then ln(0) would throw an exception. That's why we null the value in that case and use zeroifnull to convert it to 0.
                        zeroifnull(exp(sum(ln(1 - case when cessiongross < 1 then cessiongross else null end)))) as available,
                        listagg(concat('[', lowerLevel, ']: ', trim(to_varchar(cessiongross * 100, '999.00')), '%', ' {\n', CessionGrossExplanation,'\n}'), '\n') within group (order by lowerlevel asc) as AvailableExplanation
                    from 
                        grossByLevelUnderRetroAndPeriod
                    group by 
                        scenarioid, 
                        periodid,
                        retrocontractid,
                        subjectLevel
                )
                select
                    b.scenarioid,
                    b.periodid,
                    b.portlayerid,
                    b.retroblockid,
                    b.retrocontractid,
                    b.retroconfigurationid,

                    // diag factors
                    b.limit100pct,
                    b.premium100pct,
                    b.share,
                    b.sharefactor,
                    b.placement,
                    b.premiumfactor,
                    // todo: add seasonal premium factor
                    b.shareoflayerduration,
                    b.expenses,
                    b.reinstcount,
                    b.sidesign,

                    coalesce(la.available, 1) as availableAtLevel,
                    
                    AvailableExplanation,
                    
                    availableAtLevel * exposedLimit as exposedLimit,
                    availableAtLevel * exposedPremium as exposedPremium,
                    availableAtLevel * exposedExpenses as exposedExpenses,
                    availableAtLevel * proRataPremium as proRataPremium,
                    availableAtLevel * proRataPremiumExpenses as proRataPremiumExpenses,

                    // we'll need non placed blocks for calculating cession (cession gross % includes placement)
                    availableAtLevel * nonplaced_exposedLimit as nonplaced_exposedLimit,
                    availableAtLevel * nonplaced_exposedPremium as nonplaced_exposedPremium,
                    availableAtLevel * nonplaced_exposedExpenses as nonplaced_exposedExpenses,
                    
                    // todo: add seasonal premium calculation
                    availableAtLevel * nonplaced_proRataPremium as nonplaced_proRataPremium,
                    availableAtLevel * nonplaced_proRataPremiumExpenses as nonplaced_proRataPremiumExpenses
                from 
                    economic_model_computed.baseblockinfo b
                    left join availability la on la.periodid = b.periodid and la.scenarioid = b.scenarioid and la.subjectlevel = b.level and la.retrocontractid = b.retrocontractid
                    cross join currentLevel
                    -- todo: join to retrocontract (program*<->*contract) so we can distribute the blocks to contracts and we can group them later by contract
                where 
                    b.isspecific = 0 and b.level = currLevel
                    ;

        -- 2.2.2. generate and save subject blocks, using placed version of columns from economic_model_computed.subjectBlockInfo
        -- Note: I'm no longer using diffs in order to make this calculation as quick as possible. Diffs still meke sense in data sent to powerbi, but
        -- we want the Excel results to be ready as quickly as possible, so trimming down everything I can for that.

        // todo: expand subjectblock with columns for all factors we used (for auditing results)
        insert into economic_model_computed.subjectblock(
            scenarioid, retroblockid, exposedlimit, exposedrp, exposedExpenses, premiumprorata, expensesprorata, reinstcount,
            diag_limit100pct, diag_premium100pct, diag_share, diag_sharefactor, diag_placement, diag_premiumfactor, diag_shareoflayerduration, diag_expenses, diag_available, diag_available_explanation, diag_sidesign)
            select 
                scenarioid,
                retroblockid,
                exposedlimit,
                exposedpremium,
                exposedExpenses,
                proratapremium,
                proratapremiumexpenses,
                reinstcount,

                limit100pct,
                premium100pct,
                share,
                sharefactor,
                placement,
                premiumfactor,
                shareoflayerduration,
                expenses,
                availableatlevel,
                availableExplanation,
                sidesign
            from 
                economic_model_computed.subjectBlockInfo;


        // 2.2.3. calculate metrics using the subject blocks
        truncate economic_model_computed.calculationblock;

        // todo: shoud we include "exposedExpenses" here? Basically, do we use seasonal or proRata premiums when calculating required capital?
        insert into economic_model_computed.calculationblock(scenarioid, calculationcontractid, periodid, premiumyield, premiumexpenses, exposedpremium, exposedlimit)
        select scenarioid, retrocontractid, periodid, proRataPremium, proRataPremiumExpenses, exposedPremium, exposedLimit 
        from economic_model_computed.subjectblockinfo;

        call economic_model_computed.calculatecontractmetrics(null);


        
        // todo: review and update the metrics we expose here. we need to keep this table synced with the calculationcontractmetrics table, as far as included columns go
        insert into economic_model_computed.contractresult(
            scenarioid, retrocontractid, lossviewgroup, chosencapital, calculatedcapital, investedcapital, avgresult, bestresult, worstresult, avgresultpct, chanceofpositiveresult, medianresult, expectedpremium,
            expectedrp,expectedlosses, expectedpremiumtotal, maxtail, maxdailylimit, maxdailylimitagg)
            select 
                m.scenarioid, 
                calculationcontractid, 
                lossviewgroup, 
                chosencapitalformetrics, 
                requiredcapital as requiredcapital_calculated, 
                totalinvestedamount, 
                avgresult,
                bestresult, 
                worstresult,
                avgresultpct,
                chanceofpositiveresult,
                medianresult,
                expectedpremium,
                expectedrp,
                expectedlosses, 
                expectedpremiumtotal,
                maxtail,
                maxdailylimit,
                maxdailylimitagg
            from 
                economic_model_computed.calculationcontractmetrics m
                left join investedAmountByRetroContract ri on ri.scenarioid = m.scenarioid and ri.retrocontractid = m.calculationcontractid;
        

        -- 2.2.4. update the _scenario tables so we can use them to read the investor % in each block 
        -- note: we'll update the _override tables after the loop for perf reasons
        MERGE INTO
            economic_model_scenario.retrocontract_scenario r_s 
        USING 
            (select * from economic_model_computed.calculationcontractmetrics where usableForAutomaticInvestmentShareCalc = true) AS m ON 
                m.calculationcontractid = r_s.retrocontractid 
                and m.scenarioid = r_s.scenarioid 
        WHEN MATCHED THEN
             UPDATE SET r_s.targetcollateralcalculated = m.requiredCapital;

        -- todo: should we put the investment calculated % as a property of the retroinvestor, even though we're using the latest leg to get the amount?

        -- update calculated investment % for each investor
        -- we calculate the % in a temp table, so we can use it in a merge statement
        create or replace temporary table economic_model_computed.retroinvestor_calculatedinvpctdata as
            with 
                retrocontractlatestconfigid as (
                    select
                        retrocontractid,
                        max_by(retroconfigurationid, startdate) latestRetroConfigurationId
                    from
                        economic_model_staging.retroconfiguration
                    group by 
                        retrocontractid
                )
                select
                    rci.retroinvestmentlegid, 
                    rci.scenarioid, 
                    rci.investmentsignedamt / m.requiredcapital as investmentcalculatedpct
                from 
                    economic_model_computed.calculationcontractmetrics m
                    inner join retrocontractlatestconfigid lc on m.calculationcontractid = lc.retrocontractid
                    inner join economic_model_scenario.retroinvestmentleg_scenario rci on rci.retroconfigurationid = lc.latestRetroConfigurationId and rci.scenarioid = m.scenarioid
                where 
                    investmentsignedamt > 0
                    and m.usableForAutomaticInvestmentShareCalc = true
                    ;

        -- todo: see if I need to handle retros with IsSpecific=true differently as they will have InvestmentSigned=1 and the actual % will be in the retroallocation table.
        -- run the query (in the using block) and see if it makes changes to retros with IsSpecific and if they are ok.

        MERGE INTO
            economic_model_scenario.retroinvestmentleg_scenario rci_s
        using
            economic_model_computed.retroinvestor_calculatedinvpctdata as invShareOvrd on invShareOvrd.retroinvestmentlegid = rci_s.retroinvestmentlegid and invShareOvrd.scenarioid = rci_s.scenarioid
        WHEN MATCHED THEN 
             UPDATE SET rci_s.investmentcalculatedpct = invShareOvrd.investmentcalculatedpct;

        -- Now that we've updated the investmentcalculatedpct of each investor (in case the retro wasn't using REVO/Override collateral), we can finally calculate and insert ceded blocks for this level
        -- We're going to use the ceded blocks for calculating the "available" factor in the next level
        insert into economic_model_computed.cededblock(
            scenarioid, retroblockid, retrocontractinvestorid, 
            -- note: we save cessiongross for diagnostic purposes but also for calculating how much is available in the next leve. 
            -- Since I'm also using it for available I didn't add the diag_ prefix this is an internal detail so I'm on the fence about it.
            cessiongross, 
            exposedlimit, exposedrp, exposedExpenses, premiumprorata, expensesprorata, reinstcount,
            diag_limit100pct, diag_premium100pct, diag_share, diag_sharefactor, diag_placement, diag_premiumfactor, diag_shareoflayerduration, diag_expenses, diag_available, diag_available_explanation, diag_sidesign)
             select
                b.scenarioid, 
                retroblockid,
                rci.retrocontractinvestorid,
                -- note: we use the investmentsigned specified by overide/revo, or the calculated one if override/revo is not found
                b.placement * coalesce(rci.investmentsigned, rci.investmentcalculatedpct) as CessionGross,
                b.nonplaced_exposedlimit * cessiongross as exposedlimit,
                b.nonplaced_exposedpremium * cessiongross as exposedpremium,
                b.nonplaced_exposedexpenses * cessiongross as exposedExpenses,
                b.nonplaced_proratapremium * cessiongross as premiumprorata,
                b.nonplaced_proratapremiumexpenses * cessiongross as expensesprorata,
                b.reinstcount,

                limit100pct,
                premium100pct,
                share,
                sharefactor,
                placement,
                premiumfactor,
                shareoflayerduration,
                expenses,
                availableatlevel,
                availableExplanation,
                sidesign
            from 
                economic_model_computed.subjectBlockInfo b
                inner join economic_model_scenario.retroinvestmentleg_scenario rci on rci.retroconfigurationid = b.retroconfigurationid and rci.scenarioid = b.scenarioid
            where
                CessionGross > 0
        ;

    END FOR;


    // now we finish up by doing things 3, 4, and 5
   
    // 3. updating the _override tables. We needed to update the _scenario tables right away so we can use them to calculate the gross % for ceded blocks,
    // but avoided updating the _override tables in the loop for perf reasons.
    update 
        economic_model_scenario.retrocontract_override r_o 
    set 
        r_o.targetcollateralcalculated = r_s.targetcollateralcalculated
    from 
        economic_model_scenario.retrocontract_scenario r_s
    where 
        r_s.retrocontractid = r_o.retrocontractid 
        and r_s.scenarioid = r_o.scenarioid 
        and zeroifnull(r_s.targetcollateralcalculated) <> zeroifnull(r_o.targetcollateralcalculated)
        and r_s.scenarioid in (select scenarioid from economic_model_computed.scenariofiltered)
        ;
        
    update 
        economic_model_scenario.retroinvestmentleg_override rci_o
    set 
        rci_o.investmentcalculatedpct = rci_s.investmentcalculatedpct
    from 
        economic_model_scenario.retroinvestmentleg_scenario rci_s
    where 
        rci_s.retroinvestmentlegid = rci_o.retroinvestmentlegid 
        and rci_s.scenarioid = rci_o.scenarioid 
        and zeroifnull(rci_s.investmentcalculatedpct) <> zeroifnull(rci_o.investmentcalculatedpct)
        and rci_s.scenarioid in (select scenarioid from economic_model_computed.scenariofiltered)
        ;

    // 4. load ceded blocks and investor contract data and calculate investor metrics

     // 2.2.3. calculate metrics using the subject blocks
    
    truncate economic_model_computed.calculationcontract;

    insert into economic_model_computed.calculationcontract
        with investmentAmtByInvestor as (
            select distinct rl.scenarioid, rl.retrocontractinvestorid, max_by(investmentsignedamt, startdate) investmentsignedamt from economic_model_scenario.retroinvestmentleg_scenario rl
            inner join economic_model_staging.retroconfiguration rc on rl.retroconfigurationid = rc.retroconfigurationid
            inner join economic_model_staging.retrocontractinvestor rci on rl.retrocontractinvestorid = rci.retrocontractinvestorid
            group by rl.scenarioid, rl.retrocontractinvestorid
        )
        select 
            amt.scenarioid, 
            ri.retrocontractinvestorid, 
            1 /*1=retrocontract*/, 
            r.climateload, 
            r.nonmodeledload, 
            ri.brokerage, 
            ri.commission, 
            ri.profitcommission, 
            ri.reinsuranceexpensesoncededcapital, 
            ri.reinsuranceexpensesoncededpremium, 
            r.capitalcalculationlossview, 
            r.capitalcalculationtargetreturnperiod,
            1 /*capital calculation method, currently only tvar supported so this is a placeholder parameter*/,
            amt.investmentsignedamt,
            exposurestart,
            exposureend,
            case when retroprogramtype = 2 then 'RAD' else 'LOD' end
        from 
            economic_model_staging.retrocontractinvestor ri
            cross join economic_model_computed.ScenarioFiltered sf
            inner join economic_model_scenario.retrocontract_scenario r on ri.retrocontractid = r.retrocontractid and r.scenarioid = sf.scenarioid
            inner join investmentAmtByInvestor amt on amt.retrocontractinvestorid = ri.retrocontractinvestorid and amt.scenarioid = sf.scenarioid
        where 
            r.scenarioid is not null
         ;
     
    truncate economic_model_computed.calculationblock;

    insert into economic_model_computed.calculationblock(scenarioid, calculationcontractid, periodid, premiumyield, premiumexpenses, exposedpremium, exposedlimit)
    select b.scenarioid, retrocontractinvestorid, periodid, premiumprorata, expensesprorata, exposedrp, exposedLimit 
    from economic_model_computed.cededblock b
    inner join economic_model_staging.retrotag t on t.retroblockid = b.retroblockid
    inner join economic_model_computed.ScenarioFiltered sf on sf.scenarioid = b.scenarioid;

    call economic_model_computed.calculatecontractmetrics(null);
    
    // todo: review and update the metrics we expose here. we need to keep this table synced with the calculationcontractmetrics table, as far as included columns go
    insert into economic_model_computed.investorresult(
        scenarioid, retrocontractinvestorid, lossviewgroup, investedcapital, avgresult, bestresult, worstresult, avgresultpct, chanceofpositiveresult, medianresult, expectedpremium,
        expectedrp,expectedlosses, expectedpremiumtotal, maxtail, maxdailylimit, maxdailylimitagg)
        select 
            m.scenarioid, 
            calculationcontractid, 
            lossviewgroup,
            totalinvestedamount, 
            avgresult,
            bestresult, 
            worstresult,
            avgresultpct,
            chanceofpositiveresult,
            medianresult,
            expectedpremium,
            expectedrp,
            expectedlosses, 
            expectedpremiumtotal,
            maxtail,
            maxdailylimit,
            maxdailylimitagg
        from 
            economic_model_computed.calculationcontractmetrics m
            left join investedAmountByRetroContract ri on ri.scenarioid = m.scenarioid and ri.retrocontractid = m.calculationcontractid;
   

    // 5. generate gross blocks
    insert into economic_model_computed.grossblock (scenarioid, portlayerid, exposedlimit, exposedrp, premium, expenses, exposedExpenses, reinstcount)
        select 
            pl.scenarioid, 
            pl.portlayerid,
            // note: round all currency amounts to integer to save space in both snowflake and in PowerBI
            // I think this should be safe as the errors it introduce will tend to even out on aggregate and on aggregate we're interested in millions, not dollars.
            // That said, perhaps this is best reserved for the fact tables used only from powerbi.
            round(pl.limit100pct   * pl.share * s.sidesign * pl.sharefactor, 0) as exposedlimit,
            round(pl.premium100pct * pl.share * s.sidesign * pl.sharefactor, 0) as ExposedRP,
            round(pl.premium100pct * pl.share * s.sidesign * pl.sharefactor * pl.premiumfactor, 0) as Premium,
            round(Premium * pl.expenses, 0) as Expenses,
            round(ExposedRP * pl.expenses, 0) as ExposedExpenses,
            pl.reinstcount
        from 
            economic_model_computed.scenariofiltered sf
            inner join economic_model_scenario.portlayer_scenario pl on pl.scenarioid = sf.scenarioid
            inner join economic_model_staging.submission s on pl.submissionid = s.submissionid
    ;
end
;