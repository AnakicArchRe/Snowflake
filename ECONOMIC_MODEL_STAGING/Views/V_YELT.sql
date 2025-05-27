create or replace view ACTUARIAL_ILS_POC.ECONOMIC_MODEL_STAGING.V_YELT(
	LOSSANALYSISID,
	LAYERID,
	SOURCE_DB,
	EVENTID,
	PERIL,
	LOSSTYPE,
	YEAR,
	DAY,
	LOSSPCT,
	RP,
	RB,
	LOSSVIEWGROUP
) as 
        select 
            concat(source_db, '_', LossAnalysisId) as LossAnalysisId,
            concat(source_db, '_', layerid) as layerid,
            * exclude (lossanalysisid, layerid)
        from 
            economic_model_raw.v_Yelt;