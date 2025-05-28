CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.POPULATE_YELPT()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
BEGIN

    create or replace temporary table economic_model_staging.yelt as 
        select 
            concat(source_db, '_', LossAnalysisId) as LossAnalysisId,
            concat(source_db, '_', layerid) as layerid,
            * exclude (lossanalysisid, layerid)
        from 
            economic_model_raw.v_Yelt;

    create or replace temporary table economic_model_staging.yelptdata as
        with layerPeriods as 
        (
            select 
                distinct pl.layerid, pl.reinstcount, yeltperiodid, startdayofyear, enddayofyear, case when PERIODDURATION >= 365 then 1 else 0 end as AllYear
            from 
                ECONOMIC_MODEL_STAGING.portlayerperiod lp
                inner join ECONOMIC_MODEL_STAGING.portlayer pl on lp.portlayerid = pl.portlayerid
        )
        , yelpts as (
            select 
                o.layerid, 
                reinstcount,
                yeltperiodid, 
                y.LossAnalysisId LossAnalysisId, 
                LOSSVIEWGROUP,
                y.Year, 
                y.Peril, 
                sum(LossPct) TotalLoss, 
                sum(rp) TotalRp,
                sum(rb) TotalRb,
                count(*) TotalEvents
            from 
                layerPeriods o
                inner join economic_model_staging.yelt y 
                    on o.layerid = y.layerid
                    and (
                        allyear = 1
                        or (startdayofyear <= endDayofyear and startdayofyear <= y.day and y.day <= endDayofyear)
                        or (startdayofyear > endDayofyear and (y.day <= endDayofyear or y.day >= startdayofyear))
                    )
            group by 
                y.source_db, o.layerid, reinstcount, yeltperiodid, y.year, y.peril, lossviewgroup, LossAnalysisId
        )
        select
            layerid, 
            reinstcount,
            YeltPeriodId,
            LossAnalysisId,
            LOSSVIEWGROUP,
            Year, 
            Peril, 
            TotalLoss, 
            TotalRp,
            TotalRb,
            TotalEvents,
        from 
            yelpts y;

    -- Yelpt is yelt at period-level (instead of day-level)
    create or replace table ECONOMIC_MODEL_STAGING.Yelpt as
        select 
            distinct
            YeltPeriodId,
            LossAnalysisId,
            LOSSVIEWGROUP,
            Year, 
            Peril, 
            TotalLoss, 
            TotalRp,
            TotalRb,
            TotalEvents, 
            greatest(1, (reinstcount + 1) / sum(TotalLoss) over (partition by layerid, year, lossViewGroup)) as maxLossScaleFactor
        from 
            economic_model_staging.yelptdata y;
        

    create or replace table ECONOMIC_MODEL_STAGING.Seasonality as
        with layerTotalLosses as(
            -- note: we must know the total loss of a layer using the yelt. We can't rely on the previous yelpt table, because we might have overlapping periods for the same layerid, 
            -- because the layer can have inforce and projection portlayers which can be sliced into stable blocks at different days of the year.
            select layerid, lossviewgroup, sum(losspct) layerLossAllYears from economic_model_staging.yelt group by layerid, lossviewgroup
        )
        , allLossViews as (
            select distinct lossviewgroup from economic_model_staging.yelptdata
        )
        , allLayerPeriods as (
            -- note: grouping because we might have different share durations for the same layerid due to leap years (e.g. inforce non-leap, projection leap year),
            -- using min_by to force non-leap years where ambiguous.
            select yeltperiodid, layerid, min_by(shareoflayerduration, datediff(days, inception, expiration)) shareoflayerduration
            from economic_model_staging.portlayerperiod per
            inner join economic_model_staging.portlayer pl on per.portlayerid = pl.portlayerid
            group by yeltperiodid, layerid
        )
        , withExtraCols as (
            select distinct
                lp.YeltPeriodId,
                lv.LOSSVIEWGROUP,
                // periodlossallyears / layerlossallyears
                layerLossAllYears,
                sum(TotalLoss) over (partition by yd.lossViewGroup, yd.yeltperiodid) as periodLossAllYears,
                case when layerLossAllYears is null then shareoflayerduration else zeroifnull(periodLossAllYears) / layerLossAllYears end as ShareOfYearlyLayerLosses
            from 
                -- ensure we have data for all lossviewgroups and all periods, even if there were no losses in a period/lossviewgroup
                allLossViews lv
                cross join allLayerPeriods lp
                left join layerTotalLosses ltl on lp.layerid = ltl.layerid and lv.lossviewgroup = ltl.lossviewgroup
                left join economic_model_staging.yelptdata yd on lv.lossviewgroup = yd.lossviewgroup and lp.yeltperiodid = yd.yeltperiodid
        )
        select
            * exclude (layerLossAllYears, periodLossAllYears)
        from
            withExtraCols;

end
;