CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.CALCULATE_INVESTOR_RESULTS(SCENARIOID NUMBER)
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    -- TODO: extract this into a table-valued-function, we're using this same query in three queries (calculate blocks, blocksylt, investor/contract results)
    create or replace temporary table economic_model_computed.scenariofiltered as
        with dependentScenarioIds as (
            -- find all scenario ids that depend on selected scenario (self+derived scenarios)
            select distinct 
               scenarioid
            from
                economic_model_scenario.scenario_parts sp
            where
                -- partid = 7470
                partid = :scenarioId OR :scenarioId is null
        )
        -- filter out inactive ones
        select 
            s.*
        from 
            dependentscenarioids ds
            inner join economic_model_scenario.scenario s on ds.scenarioid = s.scenarioid;
           
    create or replace temporary table economic_model_computed.blockUSD as
        with
            -- break up scenarios into parts (base+diff)
            scenarioParts as (
                select 
                    p.partid partialScenarioId, 
                    sf.scenarioid,
                    // use the scenario fxdate for all parts (base+diff)
                    sf.fxdate
                from 
                    economic_model_computed.scenariofiltered sf
                    -- for each scenario row, we want to have that scenario row + base row with the same fxdate
                    -- because to get the total values for a retro, we need to add the scenario value to the base values
                    -- as the scenario value is a diff. We must make sure the added base rows share the same fxdate.
                    inner join economic_model_scenario.scenario_parts p on p.scenarioid = sf.scenarioid
            )
            select
                sp.scenarioid, 
                b.retrocontractinvestorid,
                pl.portlayerid,
                per.periodid,
                per.yeltperiodid,
                per.periodstart,
                per.periodend,
                -- (fx.rate, s.boundfxrate) as fxrate,
                sum(fx.rate * b.premiumprorata) as premiumprorata,
                sum(fx.rate * b.premiumprorata * pl.expenses) as premiumprorata_expenses,
                sum(fx.rate * b.exposedlimit) as exposedLimit,
                sum(fx.rate * b.exposedrp) as exposedRp,
                coalesce(rs.nonmodeledload, 1) as nonmodeledload,
                coalesce(rs.climateload, 1) as climateLoad
            from
                scenarioParts sp
                // get corresponding blocks for each scenario part (base and diff parts for each scenario)
                inner join economic_model_computed.cededblock b on b.scenarioid = sp.partialScenarioId 
                // get to the submission table so we can find the fxdate in case it's needed (not specified by the scenario)
                inner join economic_model_staging.retrotag t on b.retroblockid = t.retroblockid
                inner join economic_model_staging.portlayerperiod per on t.periodid = per.periodid
                inner join economic_model_staging.portlayer pl on per.portlayerid = pl.portlayerid
                inner join economic_model_staging.submission s on pl.submissionid = s.submissionid
                inner join economic_model_raw.fxrate fx on s.currency = fx.currency and fx.basecurrency = 'USD' and fx.fxdate = coalesce(sp.fxdate, s.fxdate)
                // get to the contract so we find out the climate and nonmodeledload (so can scale the ylt in the next step)
                inner join economic_model_staging.retroconfiguration rc on t.retroconfigurationid = rc.retroconfigurationid
                inner join economic_model_scenario.retrocontract_scenario rs on rc.retrocontractid = rs.retrocontractid and rs.scenarioid = b.scenarioid
            group by
                sp.scenarioid, 
                b.retrocontractinvestorid,
                pl.portlayerid,
                per.periodid,
                per.yeltperiodid,
                per.periodstart,
                per.periodend,
                rs.nonmodeledload,
                rs.climateload
    ;

    -- calculate the ylt (10k years) for each block, append premium info (yelt year independent) to each year 
    create or replace temporary table economic_model_computed.yltByInvestor as
        with
            yltUSD as (
                select
                    scenarioid,
                    retrocontractinvestorId,
                    y.lossviewgroup,
                    y.year,
                    coalesce(sum(exposedLimit * totalloss * b.nonmodeledload * least(b.climateload, y.maxlossscalefactor)), 0) as loss,
                    coalesce(sum(exposedRp * totalrp * b.nonmodeledload * least(b.climateload, y.maxlossscalefactor)), 0) as rp,
                    coalesce(sum(exposedRp * totalrb * b.nonmodeledload * least(b.climateload, y.maxlossscalefactor)), 0) as rb
                from 
                    economic_model_computed.blockUSD b
                    inner join economic_model_staging.yelpt y on b.YELTPERIODID = y.yeltperiodid
                group by 
                    scenarioid,
                    retrocontractinvestorId,
                    y.lossviewgroup,
                    y.year
            )
            -- calculate the total premium and expenses for eachh scenario/investor
            , premiumUSD as (
                select 
                    scenarioid,
                    retrocontractinvestorid,
                    sum(premiumprorata) as premiumprorata,
                    sum(premiumprorata_expenses) as premiumprorata_expenses,
                from 
                    economic_model_computed.blockUSD b
                group by
                    scenarioid,
                    retrocontractinvestorid
            )
            -- calculate losses, rb, rp (in USD) for each scenario/investor/year/lossgroup (portlayer and periods are grouped)
            select 
                y.scenarioid,
                p.retrocontractinvestorid,
                y.lossviewgroup,
                y.year,
                sum(loss) loss,
                sum(y.rp) rp,
                sum(y.rb) rb,
                sum(premiumprorata) premiumprorata,
                sum(premiumprorata_expenses) premiumprorata_expenses,
                rank() over (partition by p.retrocontractinvestorid, lossviewgroup, y.scenarioid order by sum(loss))
                    // IMPORTANT: some years might not have losses, so we start ranking years starting from the number
                    // of years without losses, so we end up going all the way to 10k in order to have data for 
                    // years 1-in-250 and 1-in-2500. If we didn't do this, in case e.g. 2k years didn't have events,
                    // we'd only go up to rank 8k and we wouldn't have data for years 1-in-250 (rank:9960) and 1-in-2500 (rank:9996).
                    // could have done a cross join on 10k years, but this seemed simpler, though this long comment defeats the point a bit...
                    + (10000 - count(year) over (partition by y.scenarioid, p.retrocontractinvestorid, lossviewgroup)) as yeltrank
            from 
                yltusd y
                -- add information about the premium to each row (premiums are not yeltyear-dependent and so were not calculated in the yltusd query)
                inner join premiumUSD p on y.scenarioid = p.scenarioid and y.retrocontractinvestorid = p.retrocontractinvestorid
            group by 
                y.scenarioid,
                p.retrocontractinvestorid,
                y.lossviewgroup,
                y.year;


    -- get required capital for retro from newest override/revo/calc value
    create or replace temporary table economic_model_computed.retrocontract_scenario_capital as
        select 
            scenarioid, retrocontractid, 
            max_by(targetcollateralcalculated, startdate) newestcalculatedcapital,
            coalesce 
            (
                max_by(targetcollateraloverride, startdate),
                max_by(targetcollateralrevo, startdate),
                newestcalculatedcapital
            ) as capitalForMetrics
        from 
            economic_model_scenario.retroconfiguration_scenario
        group by 
            scenarioid, retrocontractid;

    create or replace temporary table economic_model_computed.retrocontractinvestor_scenario_capital as
        select 
            rl.scenarioid, retrocontractinvestorid, 
            coalesce 
            (
                max_by(round(investmentsigned * capitalForMetrics, 0), startdate),
                max_by(round(investmentsignedpctcalculated * capitalForMetrics, 0), startdate)
            ) as investmentAmtForMetrics
        from 
            economic_model_scenario.retroinvestmentleg_scenario rl
            inner join economic_model_staging.retroconfiguration rc on rl.retroconfigurationid = rc.retroconfigurationid
            inner join economic_model_computed.retrocontract_scenario_capital cap on rc.retrocontractid = cap.retrocontractid and rl.scenarioid = cap.scenarioid
        group by
            rl.scenarioid, retrocontractinvestorid;

    -- A. calculate investor results
    delete from economic_model_computed.investorresult 
    where
        -- clear old data for the scenarios we're about to calculate
        scenarioid in (select scenarioid from economic_model_computed.scenariofiltered)
        -- clear any orphaned data (scenarios deleted or no longer active)
        or scenarioid not in (select scenarioid from economic_model_scenario.scenario where isactive = 1);
    
    -- create or replace table economic_model_computed.investorresult as
    insert into economic_model_computed.investorresult
        with 
            -- extend with additional calculations 
            -- (we didn't fold this into the previous query because the previous query groups rows at lower granularity)
            yltDerivedMeasures as (
                select 
                    y.scenarioid,
                    y.retrocontractinvestorid,
                    y.lossviewgroup,
                    y.year,
                    yeltrank,
                    premiumprorata,
                    -- temp
                    zeroifnull(cap.investmentAmtForMetrics) as capital,
                    premiumprorata - premiumprorata_expenses as netPremium,
                    rp - rb as netRP,
                    (1 - rci.commission - rci.brokerage) as netInvestorShare,
                    netPremium * netinvestorshare as netPremiumToInvestor,
                    netRP * netinvestorshare as netRpToInvestorFull,
                    loss,
                    (loss - netRpToInvestorFull) as lossesToCover,
                    
                    // if capital is 0 or null we have unlimited funds available for losses.
                    case when capital = 0 then lossesToCover else (capital + netPremiumToInvestor) end as availableFundsForLosses,
            
                    case when lossesToCover = 0 then 1 else least(greatest(availableFundsForLosses / lossesToCover, 0), 1) end as coveredMarginalShare,
                    netRpToInvestorFull * coveredMarginalShare as netRpToInvestorCovered,
                    netRp * coveredMarginalShare as netRpCovered,
                    loss * coveredMarginalShare as coveredLosses,
                    netPremium + netRpCovered as netPremGrossOfOverrideBase,
                    netPremGrossOfOverrideBase * rci.commission as override2,
                    netPremGrossOfOverrideBase * (1 - rci.brokerage) as netPremGrossOfOverride,
                    netPremGrossOfOverride * rci.commission as commissionamount,
                    netpremium + rp - coveredLosses as uwProfit,
                    uwProfit - commissionamount - rci.reinsuranceexpensesoncededcapital * capital - rci.reinsuranceexpensesoncededpremium * netPremGrossOfOverrideBase as profit,
                    greatest(profit, 0) * profitcommission as profitComissionAmount,
                    netPremGrossOfOverride - override2 - profitComissionAmount - coveredLosses as investorResult,
                    (loss - rp) * (1 - coveredMarginalShare) as tail
                from
                    economic_model_computed.yltByInvestor y
                    inner join economic_model_staging.retrocontractinvestor rci on y.retrocontractinvestorid = rci.retrocontractinvestorid
                    inner join economic_model_computed.retrocontractinvestor_scenario_capital cap on y.retrocontractinvestorid = cap.retrocontractinvestorid and y.scenarioid = cap.scenarioid
            )
            -- aggregate across years to get investor results
            , calculatedMeasures as (
                select
                    y.scenarioid,
                    y.retrocontractinvestorid,
                    lossviewgroup,
                    avg(capital) as investedCapital,
                    avg(y.investorresult) avgResult,
                    max(y.investorresult) bestResult,
                    min(y.investorresult) worstResult,
                    case when investedCapital > 0 then avgresult / investedCapital else null end as avgResultPct,
                    // bep = break even point
                    -- min_by(yeltrank, abs(y.investorresult)) / 10e3 as bep1,
                    1 - count_if(y.investorresult < 0) / 10e3 as chanceOfPositiveResult,
                    -- count_if(y.investorresult >= 0) / 10e3 as bep3,
                    median(y.investorresult) medianResult,
                    max(premiumprorata) expectedPremium,
                    sum(netrpcovered) / 10e3 as expectedRP,
                    sum(loss) / 10e3 as expectedLosses,
                    expectedPremium + expectedRP as expectedPremiumTotal,
                    max(tail) maxTail
                from 
                    yltDerivedMeasures y
                group by 
                    y.scenarioid, 
                    y.retrocontractinvestorid,
                    lossviewgroup
            )
            -- (we're not done yet as we need information on limits and they are aggregated across a different dimension (date instead of yelt year)
            -- so we couldn't have calculated them together with the ylt data.)
            -- Find the list of dates when the limit changes
            , limitChangeDates as (
                select distinct periodstart as date from economic_model_computed.blockUSD
                union 
                select distinct periodend as date from economic_model_computed.blockUSD
            )
            -- find the occurence and aggregate limit at each of the dates
            , limitUSDOnChangeDates as (
                select 
                    scenarioid, retrocontractinvestorid, date, sum(exposedlimit) as exposedlimit, sum(exposedlimit * (1+pl.reinstcount)) as exposedAggLimit
                from 
                    limitChangeDates d
                    inner join economic_model_computed.blockUSD b on b.periodstart <= d.date and d.date <= b.periodend
                    inner join economic_model_staging.portlayer pl on b.portlayerid = pl.portlayerid
                group by 
                    scenarioid, retrocontractinvestorid, date
            )
            -- find the highest occ/agg daily limit for each scenario/investor
            , limitDailyMaxUSD as (
                select 
                    scenarioid, retrocontractinvestorid, max(exposedlimit) as maxDailyLimit, max(exposedagglimit) as maxDailyLimitAgg
                from 
                    limitUSDOnChangeDates
                group by 
                    scenarioid, retrocontractinvestorid
            )
            select 
                -- output all investor metrics
                y.*, 
                -- and limit information (without duplicaing the join columns)
                l.* exclude (scenarioid, retrocontractinvestorid)
            from 
                calculatedmeasures y
                inner join limitDailyMaxUSD l on y.scenarioid = l.scenarioid and y.retrocontractinvestorid = l.retrocontractinvestorid
            order by 
                retrocontractinvestorid, lossviewgroup;


    -- B. calculate investor results (same calculation as A. except ylt blocks are grouped by contract)
    
    delete from economic_model_computed.contractresult
    where 
        -- clear old data for the scenarios we're about to calculate
        scenarioid in (select scenarioid from economic_model_computed.scenariofiltered)
        -- clear any orphaned data (scenarios deleted or no longer active)
        or scenarioid not in (select scenarioid from economic_model_scenario.scenario where isactive = 1);
    
    -- create or replace table economic_model_computed.contractresult as
    insert into economic_model_computed.contractresult
        with 
            yltByContract as (
                select 
                    scenarioid, 
                    retrocontractid, 
                    lossviewgroup, 
                    year, 
                    sum(loss) as loss,
                    sum(rb) as rb,
                    sum(rp) as rp,
                    sum(premiumprorata) as premiumprorata,
                    sum(premiumprorata_expenses) as premiumprorata_expenses
                from 
                    economic_model_computed.yltByInvestor y
                    inner join economic_model_staging.retrocontractinvestor rci on y.retrocontractinvestorid = rci.retrocontractinvestorid
                group by
                    scenarioid, 
                    retrocontractid, 
                    lossviewgroup, 
                    year
            ), invCapByContract as (
                select 
                    scenarioid, retrocontractid, sum(cap.investmentAmtForMetrics) InvestedCapital
                from 
                    economic_model_computed.retrocontractinvestor_scenario_capital cap
                    inner join economic_model_staging.retrocontractinvestor inv on cap.retrocontractinvestorid = inv.retrocontractinvestorid
                group by
                    scenarioid, retrocontractid
            )
            -- extend with additional calculations 
            -- (not folding this into the previous query because the previous query groups rows at lower granularity)
            , yltDerivedMeasures as (
                select 
                    y.scenarioid,
                    y.retrocontractid,
                    y.lossviewgroup,
                    y.year,
                    premiumprorata,
                    -- temp
                    zeroifnull(cap.capitalformetrics) as capital,
                    zeroifnull(cap.newestcalculatedcapital) as calculatedcapital,
                    InvestedCapital,
                    premiumprorata - premiumprorata_expenses as netPremium,
                    rp - rb as netRP,
                    (1 - commission - reinsurancebrokerageonnetpremium) as netInvestorShare,
                    netPremium * netinvestorshare as netPremiumToInvestor,
                    netRP * netinvestorshare as netRpToInvestorFull,
                    loss,
                    (loss - netRpToInvestorFull) as lossesToCover,
                    
                    // if capital is 0 or null we have unlimited funds available for losses.
                    case when capital = 0 then lossesToCover else (capital + netPremiumToInvestor) end as availableFundsForLosses,
            
                    case when lossesToCover = 0 then 1 else least(greatest(availableFundsForLosses / lossesToCover, 0), 1) end as coveredMarginalShare,
                    netRpToInvestorFull * coveredMarginalShare as netRpToInvestorCovered,
                    netRp * coveredMarginalShare as netRpCovered,
                    loss * coveredMarginalShare as coveredLosses,
                    netPremium + netRpCovered as netPremGrossOfOverrideBase,
                    netPremGrossOfOverrideBase * commission as override2,
                    netPremGrossOfOverrideBase * (1 - reinsurancebrokerageonnetpremium) as netPremGrossOfOverride,
                    netPremGrossOfOverride * commission as commissionamount,
                    netpremium + rp - coveredLosses as uwProfit,
                    uwProfit - commissionamount - reinsuranceexpensesoncededcapital * capital - rc.reinsuranceexpensesoncededpremium * netPremGrossOfOverrideBase as profit,
                    greatest(profit, 0) * profitcommission as profitComissionAmount,
                    netPremGrossOfOverride - override2 - profitComissionAmount - coveredLosses as investorResult,
                    (loss - rp) * (1 - coveredMarginalShare) as tail
                from
                    yltByContract y
                    inner join economic_model_scenario.retrocontract_scenario rc on rc.retrocontractid = y.retrocontractid and rc.scenarioid = y.scenarioid
                    inner join economic_model_computed.retrocontract_scenario_capital cap on rc.retrocontractid = cap.retrocontractid and rc.scenarioid = cap.scenarioid
                    inner join invCapByContract invcap on y.scenarioid = invcap.scenarioid and invcap.retrocontractid = y.retrocontractid
            )
            -- aggregate across years to get investor results
            , calculatedMeasures as (
                select
                    y.scenarioid,
                    y.retrocontractid,
                    lossviewgroup,
                    -- using avg since these don't vary across years (in linq it would be .distinct().single())
                    avg(capital) as RequiredCapital,
                    avg(calculatedcapital) as calculatedcapital,
                    avg(investedcapital) as investedcapital,
                    round(avg(y.investorresult), 0) avgResult,
                    round(max(y.investorresult), 0) bestResult,
                    round(min(y.investorresult), 0) worstResult,
                    case when RequiredCapital > 0 then avgresult / RequiredCapital else null end as avgResultPct,
                    1 - count_if(y.investorresult < 0) / 10e3 as chanceOfPositiveResult,
                    round(median(y.investorresult), 0) medianResult,
                    round(max(premiumprorata), 0) expectedPremium,
                    round(sum(netrpcovered) / 10e3, 0) as expectedRP,
                    round(sum(loss) / 10e3, 0) as expectedLosses,
                    round(expectedPremium + expectedRP, 0) as expectedPremiumTotal,
                    round(max(tail), 0) maxTail
                from 
                    yltDerivedMeasures y
                group by 
                    y.scenarioid, 
                    y.retrocontractid,
                    lossviewgroup
            )
            -- (we're not done yet as we need information on limits and they are aggregated across a different dimension (date instead of yelt year)
            -- so we couldn't have calculated them together with the ylt data.)
            -- Find the list of dates when the limit changes
            , limitChangeDates as (
                select distinct periodstart as date from economic_model_computed.blockUSD
                union 
                select distinct periodend as date from economic_model_computed.blockUSD
            )
            -- find the occurence and aggregate limit at each of the dates
            , limitUSDOnChangeDates as (
                select 
                    scenarioid, retrocontractid, date, sum(exposedlimit) as exposedlimit, sum(exposedlimit * (1+pl.reinstcount)) as exposedAggLimit
                from 
                    limitChangeDates d
                    inner join economic_model_computed.blockUSD b on b.periodstart <= d.date and d.date <= b.periodend
                    inner join economic_model_staging.portlayer pl on b.portlayerid = pl.portlayerid
                    inner join economic_model_staging.retrocontractinvestor rci on b.retrocontractinvestorid = rci.retrocontractinvestorid
                group by 
                    scenarioid, retrocontractid, date
            )
            -- find the highest occ/agg daily limit for each scenario/investor
            , limitDailyMaxUSD as (
                select 
                    scenarioid, retrocontractid, 
                    round(max(exposedlimit), 0) as maxDailyLimit,
                    round(max(exposedagglimit), 0) as maxDailyLimitAgg
                from 
                    limitUSDOnChangeDates
                group by 
                    scenarioid, retrocontractid
            )
            select 
                -- output all investor metrics
                y.*, 
                -- and limit information (without duplicaing the join columns)
                l.* exclude (scenarioid, retrocontractid)
            from 
                calculatedmeasures y
                inner join limitDailyMaxUSD l on y.scenarioid = l.scenarioid and y.retrocontractid = l.retrocontractid
            order by 
                retrocontractid, lossviewgroup;
end
;