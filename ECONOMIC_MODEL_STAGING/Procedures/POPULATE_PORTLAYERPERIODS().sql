CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.POPULATE_PORTLAYERPERIODS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
BEGIN

    create or replace table economic_model_staging.PortLayerPeriod as
        -- 1. break up portleayers into stable periods 
        -- (portlayer start/end, cession start/end, retro reset)
        with portlayerevents as
        (
            select portlayerid, inception as date, concat('Inception') as description, 1 as descriptionPriority from economic_model_staging.portlayer
            union
                select portlayerid, dateadd(day, 1, expiration) as date, concat('Expiration') as description, 1 as descriptionPriority from economic_model_staging.portlayer
            union
                select portlayerid, overlapstart date, concat('Started ceding to ', retroprogramid) as description, 2 as descriptionPriority 
                from economic_model_staging.portlayercession
            union
                select portlayerid, dateadd(day, 1, overlapend) date, concat('Ended ceding to ', retroprogramid) as description, 2 as descriptionPriority
                from economic_model_staging.portlayercession
        )
        , grouped as (
            select portlayerid, date, listagg(description, '; ') within group (order by DescriptionPriority) description
            from portlayerevents 
            group by portlayerid, date
            order by date
        )
        , ranked as (
            select 
                portlayerid, date, description, 
                rank() over (partition by portlayerid order by date asc) as rank
            from
                grouped
        )
        , periods as (
            select 
                r1.portlayerid, 
                r1.date as PeriodStart, 
                dateadd(day, -1, r2.date) as periodend, 
                r1.description as StartPointDescription, 
                r2.description as EndPointDescription,
                -- note: to avoid different yeltperiods for leap years (e.g. inforce is non-leap year, projection is leap year),
                -- we use a non-leap year e.g. 2025 for calculating yelt days. The yelt days go up to 365 anyway.
                dayofyear(date_from_parts(2025,month(PeriodStart), day(PeriodStart))) as PeriodStartDayOfYear_NonLeap,
                dayofyear(date_from_parts(2025,month(PeriodEnd), day(PeriodEnd))) as PeriodEndDayOfYeay_NonLeap
            from 
                ranked r1
            inner join ranked r2 
                on r1.portlayerid = r2.portlayerid
                and r1.rank = r2.rank -1
            order by 
                portlayerid, periodstart
        )
        select 
            pl.portlayerid,
            -- todo: shouldn't this include year? Probably makes no difference because next year's day-range won't overlap
            -- with this year's day-range since layers are 12months long max (todo: check if this is true and if not, include both years in PeriodId).
            concat(pl.PortLayerid, ':', dayofyear(PeriodStart), '-', dayofyear(PeriodEnd)) as PeriodId,
            -- note: yelt references layers, not portlayers, so using layerid here.
            concat(pl.LayerId, ':', PeriodStartDayOfYear_NonLeap, '-', PeriodEndDayOfYeay_NonLeap) as YeltPeriodId,
            PeriodStart, 
            PeriodEnd, 
            
            -- todo: we should really indicate that these days are non-leap year days in the column name
            PeriodStartDayOfYear_NonLeap StartDayOfYear, 
            PeriodEndDayOfYeay_NonLeap EndDayOfYear, 

            datediff(day, PeriodStart, PeriodEnd) +1 as PeriodDuration,
            (1.0 + datediff(day, PeriodStart, PeriodEnd)) / (1.0 + datediff(day, pl.Inception, pl.Expiration)) as ShareOfLayerDuration,
            StartPointDescription,
            EndPointDescription
        from
            periods p
            inner join economic_model_staging.portlayer pl on p.portlayerid = pl.portlayerid;
    
END
;