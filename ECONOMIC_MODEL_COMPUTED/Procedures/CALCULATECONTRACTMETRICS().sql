CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.CALCULATECONTRACTMETRICS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    // 1. prepare blocks in exposure period, with amounts in USD
    create or replace temporary table economic_model_computed.blockUSD as
        select
            b.scenarioid, 
            b.calculationcontractid,
            per.periodid,
            sum(fx.rate * b.premiumyield) as PremiumUSD,
            sum(fx.rate * b.premiumyield * pl.expenses) as PremiumExpensesUSD,
            sum(fx.rate * b.exposedlimit) as ExposedLimit,
            sum(fx.rate * b.exposedpremium) as ExposedPremium
        from
            economic_model_computed.calculationblock b
            // get to the submission table so we can find the fxdate in case it's needed (not specified by the scenario)
            inner join economic_model_staging.portlayerperiod per on b.periodid = per.periodid
            inner join economic_model_staging.portlayer pl on per.portlayerid = pl.portlayerid
            inner join economic_model_staging.submission s on pl.submissionid = s.submissionid
            -- todo: check if this is what PC meant with bound fx rate
            inner join economic_model_raw.fxrate fx on s.currency = fx.currency and fx.basecurrency = 'USD' and fx.fxdate = '2024-12-31'--coalesce(sp.fxdate, s.fxdate)
            -- we need this so we can get the attachment and exposureperiod
            inner join economic_model_computed.calculationcontract c on b.scenarioid = c.scenarioid and b.calculationcontractid = c.calculationcontractid
        where
            // only look at blocks that start in the exposure period
            (
                // only RAD retros can have exposure periods defined, if it's LOD we don't filter based on exposure periods
                c.attachment = 'LOD' 
                // if exposure period is not set, we don't filter out blocks based on it
                or (c.exposurestart is null and c.exposureend is null)
                // if it is an RAD retro and inception period is set (either start, stop, or both), only use the portlayers where inception is in exposure period (if specified)
                or (
                    c.attachment = 'RAD' and 
                    (
                        c.exposureStart is null 
                        OR c.exposureStart <= pl.inception
                    ) 
                    and 
                    (
                        c.exposureEnd is null 
                        OR pl.inception <= c.exposureend)
                    )
            )
        group by
            b.scenarioid, 
            b.calculationcontractid,
            per.periodid
    ;

    // We need a temp table for the ylt of each scenario/contract because we'll be reading this table twice:
    // once to calculate the required capital, and once again to calculate the contract results. If we used a cte, 
    // we'd be executing the query twice, so opting for a temp table instead to avoid this.
    create or replace temporary table economic_model_computed.yltByContract as
        with 
            yltUSD as (
                -- insert into economic_model_computed.calculateresults_out_metrics
                select
                    b.scenarioid,
                    b.calculationcontractid,
                    y.lossviewgroup,
                    y.year,
                    coalesce(sum(exposedLimit * totalloss * c.nonmodelledload * least(c.climateload, y.maxlossscalefactor)), 0) as loss,
                    coalesce(sum(exposedPremium * totalrp * c.nonmodelledload * least(c.climateload, y.maxlossscalefactor)), 0) as rp,
                    coalesce(sum(exposedPremium * totalrb * c.nonmodelledload * least(c.climateload, y.maxlossscalefactor)), 0) as rb
                from 
                    economic_model_computed.blockUSD b
                    inner join economic_model_staging.portlayerperiod per on b.periodid = per.periodid
                    inner join economic_model_staging.yelpt y on per.YELTPERIODID = y.yeltperiodid
                    inner join economic_model_computed.calculationcontract c on b.calculationcontractid = c.calculationcontractid and b.scenarioid = c.scenarioid
                group by 
                    b.scenarioid,
                    b.calculationcontractid,
                    y.lossviewgroup,
                    y.year
            )
            , premiumUSD as (
                -- calculate the total premium and expenses for each scenario/investor
                select 
                    scenarioid,
                    calculationcontractid,
                    sum(premiumusd) as premiumprorata,
                    sum(premiumexpensesusd) as premiumprorata_expenses,
                from 
                    economic_model_computed.blockUSD b
                group by
                    scenarioid,
                    calculationcontractid
            )
            -- calculate the basic TLY table (losses, rb, rp, premium, expenses) in USD for each scenario/contract/year/lossgroup (portlayer and periods are grouped)
            select 
                y.scenarioid,
                p.calculationcontractid,
                y.lossviewgroup,
                y.year,
                sum(loss) loss,
                sum(y.rp) rp,
                sum(y.rb) rb,
                sum(premiumprorata) premiumprorata,
                sum(premiumprorata_expenses) premiumprorata_expenses/*,
                rank() over (partition by p.calculationcontractid, lossviewgroup, y.scenarioid order by sum(loss))
                    // IMPORTANT: some years might not have losses, so we start ranking years starting from the number
                    // of years without losses, so we end up going all the way to 10k in order to have data for 
                    // years 1-in-250 and 1-in-2500. If we didn't do this, in case e.g. 2k years didn't have events,
                    // we'd only go up to rank 8k and we wouldn't have data for years 1-in-250 (rank:9960) and 1-in-2500 (rank:9996).
                    // could have done a cross join on 10k years, but this seemed simpler.
                    + (10000 - count(year) over (partition by y.scenarioid, p.calculationcontractid, lossviewgroup)) as yeltrank*/
            from 
                yltusd y
                -- add information about the premium to each row (premiums are not yeltyear-dependent and so were not calculated in the yltusd query)
                inner join premiumUSD p on y.scenarioid = p.scenarioid and y.calculationcontractid = p.calculationcontractid
            group by 
                y.scenarioid,
                p.calculationcontractid,
                y.lossviewgroup,
                y.year;


    create or replace table economic_model_computed.calculationcontractmetrics as
        // Once we have the ytl for each contract/scenario, we can calculate the required collateral, and then the metrics. 
        // We calculate the required collateral first, because it will be used for calculating metrics for contracts that do not have revo/override collateral set.
        with
            -- note: we need to calculate the required collateral first, because we'll be using it for calculating all other metrics
            -- in case the contract does not specify a collateral amount
            capitalCalculationYLT as (
                -- first extract what we need to calculate the required capital
                select 
                    b.scenarioid,
                    lossviewgroup,
                    year, 
                    b.calculationcontractid,
                    sum(premiumprorata) as premium,
                    sum(premiumprorata_expenses) as expenses,
                    sum(loss) as losses,
                    sum(rp) as totalrp,
                    sum(rb) as totalrb,
                    premium - expenses + totalrp - totalrb as netPremium,
                    CAPITALCALCULATIONTARGETRETURNPERIOD,
                    netPremium * (1 - c.REINSURANCEBROKERAGEONNETPrEMIUM - commissionOnnetPremium) as availablePremiumForLosses,
                    losses - availablePremiumForLosses as netresult,
                    round(greatest(0, netresult), 0) as shortfall,
                    rank() over (partition by b.scenarioid, lossviewgroup, b.calculationcontractid order by netresult desc) rnk
                from 
                    economic_model_computed.yltByContract b
                    inner join economic_model_computed.calculationcontract c on b.calculationcontractid = c.calculationcontractid and b.scenarioid = c.scenarioid            
                group by 
                    b.scenarioid,
                    lossviewgroup,
                    year,
                    b.calculationcontractid,
                    c.commissiononnetpremium,
                    CAPITALCALCULATIONTARGETRETURNPERIOD, REINSURANCEBROKERAGEONNETPrEMIUM
            )
            , requiredCapitalCalc as (
                -- now calculate the required capital (todo: support other calculation methods)
                select 
                    scenarioid, lossviewgroup, calculationcontractid, round(avg(case when rnk <= CAPITALCALCULATIONTARGETRETURNPERIOD then shortfall else null end)) requiredCapital, 
                from 
                    capitalCalculationYLT
                group by 
                    scenarioid, lossviewgroup, calculationcontractid
            )
            , yltDerivedMeasures as (
                -- now prepare everything we need (still at the ylt level) for calculating metrics
                select 
                    y.scenarioid,
                    y.calculationcontractid,
                    y.lossviewgroup,
                    y.year,
                    premiumprorata,
                    rc.requiredcapital,
                    // using the required collateral as a fallback option
                    zeroifnull(coalesce(c.availablecapital, rc.requiredcapital)) as capital,
                    premiumprorata - premiumprorata_expenses as netPremium,
                    rp - rb as netRP,
                    (1 - c.commissiononnetpremium - c.reinsurancebrokerageonnetpremium) as netinvestorshare,
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
                    netPremGrossOfOverrideBase * c.commissiononnetpremium as override2,
                    netPremGrossOfOverrideBase * (1 - c.reinsurancebrokerageonnetpremium) as netPremGrossOfOverride,
                    netPremGrossOfOverride * c.commissiononnetpremium as commissionamount,
                    netpremium + rp - coveredLosses as uwProfit,
                    uwProfit - commissionamount - c.reinsuranceexpensesoncededcapital * capital - c.reinsuranceexpensesoncededpremium * netPremGrossOfOverrideBase as profit,
                    greatest(profit, 0) * profitcommission as profitComissionAmount,
                    netPremGrossOfOverride - override2 - profitComissionAmount - coveredLosses as investorResult,
                    (loss - rp) * (1 - coveredMarginalShare) as tail
                from
                    economic_model_computed.yltByContract y
                    inner join economic_model_computed.calculationcontract c on y.calculationcontractid = c.calculationcontractid and y.scenarioid = c.scenarioid
                    inner join requiredCapitalCalc rc on y.scenarioid = rc.scenarioid and y.lossviewgroup = rc.lossviewgroup and y.calculationcontractid = rc.calculationcontractid
            )
            , calculatedmeasures as (
                select
                    y.scenarioid,
                    y.calculationcontractid,
                    y.lossviewgroup,
                    avg(requiredcapital) as requiredcapital,
                    avg(capital) as chosenCapitalForMetrics,
                    avg(y.investorresult) avgResult,
                    max(y.investorresult) bestResult,
                    min(y.investorresult) worstResult,
                    case when chosenCapitalForMetrics > 0 then avgresult / chosenCapitalForMetrics else null end as avgResultPct,
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
                    y.calculationcontractid,
                    y.lossviewgroup
            )
            -- (we're not done yet as we need information on limits and they are aggregated across a different dimension (date instead of yelt year)
            -- so we couldn't have calculated them together with the ylt data.)
            -- Find the list of dates when the limit changes
            , periods as (
                select periodstart, periodend from economic_model_computed.blockUSD b
                inner join economic_model_staging.portlayerperiod per on b.periodid = per.periodid
            )
            , limitChangeDates as (
                select distinct periodstart as date from periods
                union 
                select distinct periodend as date from periods
            )
            -- find the occurence and aggregate limit at each of the dates
            , limitUSDOnChangeDates as (
                select 
                    scenarioid, calculationcontractid, date, 
                    sum(exposedlimit) as exposedlimit, sum(exposedlimit * (1 + pl.reinstcount)) as exposedAggLimit
                from 
                    limitChangeDates d
                    cross join economic_model_computed.blockUSD b
                    -- todo: investigate if including the portlayer and period columns early on (in blocksylt) improves perf by reducing the need for these subequent joins
                    inner join economic_model_staging.portlayerperiod per on b.periodid = per.periodid and per.periodstart <= d.date and d.date <= per.periodend
                    inner join economic_model_staging.portlayer pl on per.portlayerid = pl.portlayerid
                group by 
                    scenarioid, calculationcontractid, date
            )
            -- find the highest occ/agg daily limit for each scenario/investor
            , limitDailyMaxUSD as (
                select 
                    scenarioid, calculationcontractid, round(max(exposedlimit)) as maxDailyLimit, round(max(exposedagglimit)) as maxDailyLimitAgg
                from 
                    limitUSDOnChangeDates
                group by 
                    scenarioid, calculationcontractid
            )
            select 
                -- output all investor metrics
                y.*, 
                -- and limit information (without duplicaing the join columns)
                l.* exclude (scenarioid, calculationcontractid),
                -- indicates if the required capital for this lossview should be used for automatic investment % calculation ()
                (y.lossviewgroup = capitalcalculationlossview) as usableForAutomaticInvestmentShareCalc,
            from 
                calculatedmeasures y
                inner join limitDailyMaxUSD l on y.scenarioid = l.scenarioid and y.calculationcontractid = l.calculationcontractid            
                inner join economic_model_computed.calculationcontract c on c.scenarioid = y.scenarioid and c.calculationcontractid = y.calculationcontractid
            order by 
                calculationcontractid, lossviewgroup;

end
;