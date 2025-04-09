CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.POPULATE_FX(SCENARIOID NUMBER)
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    create or replace table economic_model_staging.fxrate as
        with 
            -- generate dates
            dates as (
                select dateadd(day, seq4(), (select min (fxdate) from revo_bermuda.dbo.fxrate)) date from table (generator(rowcount => (select 10e3)))
                where date < (select max(fxdate) from economic_model_raw.submission)
            )
            -- get fx records
            , fxRows as (
                select * from economic_model_raw.fxrate
            )
            -- get list of currency combinations
            , currencies as (
                select distinct currency, basecurrency from fxrows
            )
            -- for each combination of date and currency, find the latest value
            ,cte as (
                select 
                    d.date as fxdate, 
                    c.currency,
                    c.basecurrency,
                    fx.rate
                from
                    dates d
                    cross join currencies c
                    left outer join fxRows fx on d.date = fx.fxdate and c.currency = fx.currency and fx.basecurrency = c.basecurrency
            )
            select 
                fxdate, currency, basecurrency,
                coalesce(rate, lead(rate) ignore nulls over (partition by currency, basecurrency order by cte.fxdate desc)) rate
            from
                cte 
            order by 
                fxdate desc, 
                basecurrency asc, 
                currency asc;
end
;