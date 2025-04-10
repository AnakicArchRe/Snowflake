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
    insert into economic_model_computed.blockoperations_in(scenarioid, blockid, exposedlimit, exposedrp, premiumprorata, expensesprorata, exposedpremium, exposedexpenses)
        select 
            scenarioid, portlayerid, exposedlimit, exposedrp, premium, expenses, exposedrp, exposedexpenses
        from 
            economic_model_computed.grossblock;

    call economic_model_computed.blockoperations_reducetodiff();

    truncate economic_model_computed.grossblockylt;

    insert into economic_model_computed.grossblockylt(scenarioid, lossviewgroup, year, peril, portlayerid, loss, rp, rb)
        select 
            b.scenarioid,
            y.lossviewgroup,
            y.year,
            y.peril,
            pl.portlayerid,
            -- cast(per.periodstart as date) periodstart,
            -- cast(per.periodend as date) periodend,
            round(sum(exposedlimit * totalloss))  Loss,
            round(sum(exposedrp * totalrp))  RP,
            round(sum(exposedrp * totalrb))  RB
        from 
            economic_model_computed.blockoperations_out b
            inner join economic_model_staging.portlayer pl on b.blockid = pl.portlayerid
            inner join economic_model_staging.portlayerperiod per on pl.portlayerid = per.portlayerid
            inner join economic_model_staging.yelpt y on per.yeltperiodid = y.yeltperiodid
        group by
            year, peril, lossviewgroup, b.scenarioid, pl.portlayerid
    ;
       
end
;