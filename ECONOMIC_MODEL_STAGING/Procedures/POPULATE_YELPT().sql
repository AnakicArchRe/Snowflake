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
            -- note: we must know the total loss of a layer using the yelt. We can't rely on the previous yelpt table, because we might have overlapping periods for the same layerid, because 
            -- we might have overlapping yelt periods for the same layer because the layer can have inforce and projections, and they will be sliced into stable blocks at different days of the year.
            select layerid, lossviewgroup, sum(losspct) layerLossAllYears from economic_model_staging.yelt group by layerid, lossviewgroup
        )
        select
            distinct
            yd.YeltPeriodId,
            yd.LOSSVIEWGROUP,
            // periodlossallyears / layerlossallyears
            sum(TotalLoss) over (partition by yd.lossViewGroup, yd.yeltperiodid) / layerLossAllYears as ShareOfYearlyLayerLosses
        from 
            economic_model_staging.yelptdata yd
            inner join layerTotalLosses ltl on yd.layerid = ltl.layerid and yd.lossviewgroup = ltl.lossviewgroup
        ;

end
;