CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.CALCULATE_SUBJECTBLOCKSYLT()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
BEGIN

    truncate economic_model_computed.blockoperations_in;
    
    // todo: consider removing premiums and expenses from this table, because we no longer use diffs for non-ylt blocks.
    // todo: also consider moving the diff operations into a separate schema for report optimizations (economic_model_reporting, along with ylt tables and procedures).
    insert into economic_model_computed.blockoperations_in(scenarioid, blockid, exposedlimit, exposedrp, premiumprorata, expensesprorata, exposedpremium, exposedexpenses)
        select 
            b.scenarioid, retroblockid, exposedlimit, exposedrp, premiumprorata, expensesprorata, exposedrp, exposedexpenses
        from 
            economic_model_computed.subjectblock b
            // filter for active scenarios
            inner join economic_model_scenario.scenario s on b.scenarioid = s.scenarioid and s.isactive = 1;

    call economic_model_computed.blockoperations_reducetodiff();

    truncate economic_model_computed.subjectblockylt;
    
    insert into economic_model_computed.subjectblockylt (year, peril, lossviewgroup, portlayerid, retrocontractid, scenarioid, lockedfxrate, subjectLoss, subjectRP, subjectRB, maxlossscalefactor)
        with cte as (
            select
                y.year,
                y.peril,
                y.lossviewgroup, 
                pl.portlayerid, 
                rcf.retrocontractid, 
                rb.scenarioid,
                pl.lockedfxrate,
                coalesce(rcs.nonmodeledload, 1) * least(coalesce(rcs.climateload, 1), y.maxlossscalefactor) as scaleFactor,
                round(sum(scaleFactor * exposedlimit * totalloss))  subjectLoss,
                round(sum(scaleFactor * exposedrp * totalrp)) subjectRP,
                round(sum(scaleFactor * exposedrp * totalrb)) subjectRB,
                y.maxlossscalefactor / scalefactor as maxlossscalefactor
            from 
                economic_model_computed.blockoperations_out rb
                inner join economic_model_staging.retrotag t on rb.blockid = t.retroblockid
                inner join economic_model_staging.retroconfiguration rcf on t.retroconfigurationid = rcf.retroconfigurationid
                inner join economic_model_staging.portlayerperiod per on t.periodid = per.periodid
                inner join economic_model_staging.yelpt y on per.yeltperiodid = y.yeltperiodid
                inner join economic_model_computed.portlayer_scenario pl on per.portlayerid = pl.portlayerid and rb.scenarioid = pl.scenarioid
                inner join economic_model_computed.retrocontract_scenario rcs on rcf.retrocontractid = rcs.retrocontractid and rcs.scenarioid = rb.scenarioid
            where
                // The reason for this is that SubjectYLT is per-retro, so there are many millions of rows. To reduce the amount of data sent to powerbi, we skip
                // calculation of the subject ylt blocks that are outside the retros specified by the scenario. We do not check this flag for net and gross YLT blocks
                // because they are not per-retro, so there are not as many row there.
                rcs.includeinanalysis = true
            group by
                y.year, 
                y.peril, 
                y.lossviewgroup, 
                pl.portlayerid, 
                rcf.retrocontractid, 
                rb.scenarioid,
                pl.lockedfxrate,
                y.maxlossscalefactor, 
                rcs.nonmodeledload,
                rcs.climateload
        )
        select 
            * exclude(scaleFactor)
        from 
            cte
        ;
    
    create or replace table economic_model_computed.subjectblock_seasonal_premium as
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