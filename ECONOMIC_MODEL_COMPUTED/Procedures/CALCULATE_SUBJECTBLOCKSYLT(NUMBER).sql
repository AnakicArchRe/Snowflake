CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.CALCULATE_SUBJECTBLOCKSYLT(SCENARIOID NUMBER)
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

    call economic_model_computed.blockoperations_reducetodiff(:scenarioId);


    call economic_model_computed.clearscenariodatafromtable('subjectblockylt', :scenarioId);
    
    insert into economic_model_computed.subjectblockylt (year, peril, lossviewgroup, portfolioid, topupzoneid, facility, inceptionMonth, retrocontractid, scenarioid, subjectLoss, subjectRP, subjectRB)
        select
            y.year,
            y.peril,
            y.lossviewgroup,
            pl.portfolioid, 
            pl.topupzoneid,
            pl.facility,
            month(pl.inception) as inceptionMonth,
            rcf.retrocontractid, 
            rb.scenarioid,
            round(sum(coalesce(rcs.nonmodeledload, 1) * least(coalesce(rcs.climateload, 1), y.maxlossscalefactor) * exposedlimit * totalloss)) subjectLoss,
            round(sum(coalesce(rcs.nonmodeledload, 1) * least(coalesce(rcs.climateload, 1), y.maxlossscalefactor) * exposedrp * totalrp)) subjectRP,
            round(sum(coalesce(rcs.nonmodeledload, 1) * least(coalesce(rcs.climateload, 1), y.maxlossscalefactor) * exposedrp * totalrb)) subjectRB
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
            pl.portfolioid, 
            pl.topupzoneid,
            pl.facility,
            month(pl.inception),
            rcf.retrocontractid, 
            rb.scenarioid
        ;

    call economic_model_computed.clearscenariodatafromtable('subjectblock_seasonal_premium', :scenarioId);
    insert into economic_model_computed.subjectblock_seasonal_premium(retroblockid, scenarioid, lossviewgroup, premiumSeasonal, expensesSeasonal)
        select 
            t.retroblockid, 
            rb.scenarioid, se.lossviewgroup, 
            rb.exposedpremium * se.shareofyearlylayerlosses premiumSeasonal, 
            rb.exposedexpenses * se.shareofyearlylayerlosses expensesSeasonal,
        from
            economic_model_computed.blockoperations_out rb
            inner join economic_model_staging.retrotag t on rb.blockid = t.retroblockid
            inner join economic_model_staging.portlayerperiod per on t.periodid = per.periodid
            inner join economic_model_staging.portlayer pl on per.portlayerid = pl.portlayerid
            inner join economic_model_staging.seasonality se on se.yeltperiodid = per.yeltperiodid
    ;

end
;