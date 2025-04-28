CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.POPULATE_PORTLAYERPERIODS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
BEGIN

    create or replace temporary table economic_model_staging.PortLayerPeriod_without12MonthCuts as
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
        select 
            r1.portlayerid, 
            r1.date as PeriodStart, 
            dateadd(day, -1, r2.date) as periodend, 
            r1.description as StartPointDescription, 
            r2.description as EndPointDescription
        from 
            ranked r1
        inner join ranked r2 
            on r1.portlayerid = r2.portlayerid
            and r1.rank = r2.rank -1
        order by 
            portlayerid, periodstart
    ;

    create or replace table economic_model_staging.PortLayerPeriod as
        with recursive longBlockParts as (
            select 
                portlayerid, 
                dateadd(year, 1, periodstart) as nextPeriodStart, 
                periodEnd as originalPeriodEnd,
                periodstart as newPeriodStart,
                dateadd(day, -1, nextPeriodStart) as newPeriodEnd,
                StartPointDescription as StartPointDescription,
                'Block 12 month max cutoff' as EndPointDescription
            from 
                economic_model_staging.PortLayerPeriod_without12MonthCuts
            where 
                nextPeriodStart <= originalPeriodEnd
            union all
            select 
                portlayerid, 
                dateadd(year, 1, nextPeriodStart) as this_nextPeriodStart, 
                originalPeriodEnd,
                nextPeriodStart as newPeriodStart,
                dateadd(day, -1, this_nextPeriodStart) as newPeriodEnd,
                'Block 12 month max cutoff' as StartPointDescription,
                'Block 12 month max cutoff' as EndPointDescription
            from 
                longBlockParts
            where 
                this_nextPeriodStart < originalPeriodEnd
        )
        , PortLayerPeriodData as (
            -- ones shorter than a year can stay, but the ones longer than that must be replaced with their parts
            select
                portlayerid, 
                PeriodStart, 
                periodend, 
                StartPointDescription, 
                EndPointDescription
            from 
                economic_model_staging.PortLayerPeriod_without12MonthCuts
            where
                dateadd(year, 1, periodStart) > periodend
            union
            -- add new blocks created by breaking up the ones longer than 12 months
            select 
                portlayerid, 
                newPeriodStart, 
                newPeriodEnd,
                StartPointDescription,
                EndPointDescription
            from 
                longBlockParts
        )
        , withNonLeapDates as (
            select
                *,
                -- note: to avoid different yeltperiods for leap years (e.g. inforce is non-leap year, projection is leap year),
                -- we use a non-leap year e.g. 2025 for calculating yelt days. More importantly, this is also because yelt days go up to 365.
                dayofyear(date_from_parts(2025,month(PeriodStart), day(PeriodStart))) as PeriodStartDayOfYear_NonLeap,
                dayofyear(date_from_parts(2025,month(PeriodEnd), day(PeriodEnd))) as PeriodEndDayOfYeay_NonLeap
            from
                PortLayerPeriodData
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
            -- note: this cast is important, without it we get rounding errors that cause visible discrepancies, even for small integer division (<=365)
            -- For example, the seasonal and pro-rata premiums should be the same for RAD retros, but were visibly different already at the 3rd decimal place
            -- due to accumulated errors caused by low precision rounding here.
            cast(PeriodDuration as float) / (1.0 + datediff(day, pl.Inception, pl.Expiration)) as ShareOfLayerDuration,
            StartPointDescription,
            EndPointDescription
        from
            withNonLeapDates p
            inner join economic_model_staging.portlayer pl on p.portlayerid = pl.portlayerid;
    
END
;