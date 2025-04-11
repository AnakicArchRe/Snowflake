CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.CALCULATE_NETBLOCKSYLT()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
BEGIN

    // 1. calculate net diff blocks
    truncate economic_model_computed.blockoperations_in;

    // todo: rename columns to be agnostic about the kind of blocks, e.g. premiumprorata to premium
    insert into economic_model_computed.blockoperations_in(scenarioid, blockid, exposedlimit, exposedrp, premiumprorata, expensesprorata)
        select 
            // todo: should this be using cedeblockid (composed of retroblock+investorid) instead of retroblockid?
            b.scenarioid, retroblockid, exposedlimit, exposedrp, premiumprorata, expensesprorata
        from 
            economic_model_computed.cededblock b
            // filter for active scenarios
            inner join economic_model_scenario.scenario s on b.scenarioid = s.scenarioid and s.isactive = 1
        where 
            retrocontractinvestorid = 'NET_POSITION_INVESTOR'
    ;

    call economic_model_computed.blockoperations_reducetodiff();

    // 2. generate YLT for the net diff blocks
    truncate economic_model_computed.netblockylt;
        
    insert into economic_model_computed.netblockylt(scenarioid, lossviewgroup, year, peril, portlayerid, periodstart, periodend, loss, rp, rb)
        select 
            b.scenarioid,
            y.lossviewgroup,
            y.year,
            y.peril,
            pl.portlayerid,
            per.periodstart,
            per.periodend,
            round(sum(exposedlimit * totalloss * coalesce(rcs.nonmodeledload, 1) * least(coalesce(rcs.climateload, 1), y.maxlossscalefactor)))  loss,
            round(sum(exposedrp * totalrp * coalesce(rcs.nonmodeledload, 1) * least(coalesce(rcs.climateload, 1), y.maxlossscalefactor)))  RP,
            round(sum(exposedrp * totalrb * coalesce(rcs.nonmodeledload, 1) * least(coalesce(rcs.climateload, 1), y.maxlossscalefactor)))  RB
        from 
            economic_model_computed.blockoperations_out b
            inner join economic_model_staging.retrotag t on b.blockid = t.retroblockid
            inner join economic_model_staging.portlayerperiod per on t.periodid = per.periodid
            inner join economic_model_staging.yelpt y on per.yeltperiodid = y.yeltperiodid
            inner join economic_model_staging.portlayer pl on per.portlayerid = pl.portlayerid
            inner join economic_model_staging.retroconfiguration rcf on t.retroconfigurationid = rcf.retroconfigurationid
            inner join economic_model_computed.retrocontract_scenario rcs on rcf.retrocontractid = rcs.retrocontractid and rcs.scenarioid = b.scenarioid
        // Note: commented out because we calculate net ylt for all scenarios, only the subjectylt calculation depends on the includeinanalysis flag.
        // The reason for this is that SubjectYLT is per-retro, so there are many more rows. To reduce the amount of data sent to powerbi, we skip
        // calculation of the subject ylt blocks that are outside the selected retros. We calculate net and gross YLT blocks regardless of if they
        // are explicitly included in the scenario. 
        -- where
        --     rcs.includeinanalysis = true and
        group by
            year, peril, lossviewgroup, b.scenarioid, pl.portlayerid, per.periodstart, per.periodend
    ;

    -- this is currently simple, but if, for some reason it gets more involved, consider extracting this into separate procedure, as all three _ylt procs have this.
    create or replace table economic_model_computed.netblock_seasonal_premium as
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