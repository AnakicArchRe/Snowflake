CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.CALCULATE_GROSSBLOCKSYLT()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
BEGIN

    -- create or replace table economic_model_computed.grossblockylt 
    -- (
    --     scenarioid int,
    --     lossviewgroup varchar (10),
    --     year int,
    --     peril varchar(10),
    --     portlayerid varchar(30),
    --     Loss int,
    --     RP int,
    --     RB int
    -- );

    truncate economic_model_computed.blockoperations_in;

    // todo: rename columns to be agnostic about the kind of blocks, e.g. premiumprorata to premium
    // todo #2: now that we've removed the diff operations from non-ylt blocks, we might as well remove premiums and expenses from the blockoperations table entirely since they are not used for ylt blocks.
    insert into economic_model_computed.blockoperations_in(scenarioid, blockid, exposedlimit, exposedrp, premiumprorata, expensesprorata, exposedpremium, exposedexpenses)
        select 
            b.scenarioid, portlayerid, exposedlimit, exposedrp, premium, expenses, exposedrp, exposedexpenses
        from 
            economic_model_computed.grossblock b
            // filter for active scenarios
            inner join economic_model_scenario.scenario s on b.scenarioid = s.scenarioid and s.isactive = 1;

    call economic_model_computed.blockoperations_reducetodiff();

    truncate economic_model_computed.grossblockylt;

    insert into economic_model_computed.grossblockylt(scenarioid, lossviewgroup, year, peril, portlayerid, lockedFxRate, loss, rp, rb)
        select 
            b.scenarioid,
            y.lossviewgroup,
            y.year,
            y.peril,
            pl.portlayerid,
            
            lockedFxRate,

            round(sum(exposedlimit * totalloss))  Loss,
            round(sum(exposedrp * totalrp))  RP,
            round(sum(exposedrp * totalrb))  RB,
        from 
            economic_model_computed.blockoperations_out b
            inner join economic_model_computed.portlayer_scenario pl on b.blockid = pl.portlayerid and b.scenarioid = pl.scenarioid
            inner join economic_model_staging.portlayerperiod per on pl.portlayerid = per.portlayerid
            inner join economic_model_staging.yelpt y on per.yeltperiodid = y.yeltperiodid
        group by
            year, peril, lossviewgroup, b.scenarioid, pl.portlayerid, lockedfxrate
    ;

    -- this is currently simple, but if, for some reason it gets more involved, consider extracting this into separate procedure, as all three _ylt procs have this.
    create or replace table economic_model_computed.grossblock_seasonal_premium as
        select 
            t.retroblockid, rb.scenarioid, se.lossviewgroup, rb.exposedpremium * se.shareofyearlylayerlosses premiumSeasonal, rb.exposedexpenses * se.shareofyearlylayerlosses expensesSeasonal
        from
            economic_model_computed.blockoperations_out rb
            inner join economic_model_staging.retrotag t on rb.blockid = t.retroblockid
            inner join economic_model_staging.portlayerperiod per on t.periodid = per.periodid
            inner join economic_model_staging.seasonality se on se.yeltperiodid = per.yeltperiodid
    ;
       
end
;