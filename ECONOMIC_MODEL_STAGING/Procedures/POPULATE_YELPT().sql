CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.POPULATE_YELPT()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
$$
BEGIN

    -- Yelpt is yelt at period-level (instead of day-level)
    create or replace table ECONOMIC_MODEL_STAGING.Yelpt as
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
            concat(y.source_db, '_', y.LossAnalysisId) LossAnalysisId, 
            LOSSVIEWGROUP,
            y.Year, 
            y.Peril, 
            sum(LossPct) TotalLoss, 
            sum(rp) TotalRp,
            sum(rb) TotalRb,
            count(*) TotalEvents
        from 
            layerPeriods o
            inner join economic_model_raw.v_Yelt y 
                on o.layerid = concat(y.source_db, '_', y.layerid)
                and (
                    allyear = 1
                    or (startdayofyear <= endDayofyear and startdayofyear <= y.day and y.day <= endDayofyear)
                    or (startdayofyear > endDayofyear and (y.day <= endDayofyear or y.day >= startdayofyear))
                )
        group by 
            y.source_db, o.layerid, reinstcount, yeltperiodid, y.year, y.peril, lossviewgroup, LossAnalysisId
    )
    , withYearlyLoss as (
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
            sum(TotalLoss) over (partition by layerid, year, lossViewGroup, yeltperiodid) as periodLoss,
            sum(TotalLoss) over (partition by layerid, year, lossViewGroup) as yearlyLoss
        from 
            yelpts y
    )
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
        greatest(1, (reinstcount + 1) / yearlyLoss) as maxLossScaleFactor,
        -- what percentage of the layer's yearly losses is this period responsible for
        periodLoss / yearlyLoss as PeriodLossShare
    from 
        withYearlyLoss y;
end
$$;