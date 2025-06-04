create or replace view ACTUARIAL_ILS_POC.ECONOMIC_MODEL_RAW.V_YELT(
	SOURCE_DB,
	LOSSANALYSISID,
	LAYERID,
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
    
    -- todo: review this code, see if we can/should use the sources table
    -- and dynamically load from the sources listed there. Alternatively, we might 
    -- want to remove this view alltogether and rely join where appropriate

    with layerLossAnalyses_arl_ranked as
    (
        SELECT
              A.LAYERID, 
              A.LOSSANALYSISID, 
              RANK() OVER (PARTITION BY A.LAYERID, V.LOSSVIEWGROUP ORDER BY V.LOSSVIEWPRIORITY DESC) AS RANKVIEW,
              LossViewGroup
        FROM 
            REVO_bermuda.DBO.LAYERLOSSANALYSIS A
            inner join REVO_bermuda.dbo.layer l on a.layerid = l.layerid
            inner join (
                SELECT DISTINCT 
                    LOSSANALYSISID, 
                    LOSSVIEW, 
                    CASE 
                        WHEN LOSSVIEW in (4, 44) THEN 'CLIENT' 
                        WHEN LOSSVIEW IN (1, 10, 11) THEN 'ARCH' 
                        WHEN LOSSVIEW IN (3, 30, 33) THEN 'STRESSED' 
                    END AS LOSSVIEWGROUP,
                    CASE 
                        // "revised" is highest priority, then "regular", then "budget"
                        WHEN LOSSVIEW in (10,30,40) THEN 3 
                        WHEN LOSSVIEW IN (1, 3, 4) THEN 2 
                        WHEN LOSSVIEW IN (11, 33, 44) THEN 1 
                    END AS LOSSVIEWPRIORITY
                FROM 
                    REVO_bermuda.DBO.LOSSANALYSIS
                WHERE
                    LOSSVIEW IN (1, 10, 11, 3, 30, 33, 4, 44)
                    and isactive = 1 
                    and isdeleted = 0 
                    and MODEL= 'AIR'
           ) V ON V.LOSSANALYSISID = A.LOSSANALYSISID
        WHERE 
            l.status in (4,10,21,22,23,31,27,32,33,36) 
            and segment in ('PC') 
            and A.ISACTIVE  = 1 
    )
    ,layerLossAnalyses_arl as (
        select layerid, lossanalysisid, LossViewGroup from layerLossAnalyses_arl_ranked where rankview = 1
	),
    layerYelt_arl as 
	(
		select 
            'ARL',
            la.LossAnalysisId, 
            la.LayerId, 
            EventId, 
            Peril, 
            LossType, 
            Year, 
            Day, 
            LossPct, 
            RP, 
            RB,
            LossViewGroup
		from 
            layerLossAnalyses_arl la
            inner join REVOLAYERLOSS_bermuda.DBO.LAYERYELT Y on Y.LossAnalysisId = la.LossAnalysisId AND y.layerid = la.layerid
		where losstype = 1
	),
    layerLossAnalyses_arc_ranked as
    (
        SELECT
              A.LAYERID, 
              A.LOSSANALYSISID, 
              RANK() OVER (PARTITION BY A.LAYERID, V.LOSSVIEWGROUP ORDER BY V.LOSSVIEWPRIORITY DESC) AS RANKVIEW,
              LossViewGroup
        FROM 
            REVO_morristown.DBO.LAYERLOSSANALYSIS A
            inner join REVO_morristown.dbo.layer l on a.layerid = l.layerid
            inner join (
                SELECT DISTINCT 
                    LOSSANALYSISID, 
                    LOSSVIEW, 
                    CASE 
                        WHEN LOSSVIEW in (4, 44) THEN 'CLIENT' 
                        WHEN LOSSVIEW IN (1, 10, 11) THEN 'ARCH' 
                        WHEN LOSSVIEW IN (3, 30, 33) THEN 'STRESSED' 
                    END AS LOSSVIEWGROUP,
                    CASE 
                        // "revised" is highest priority, then "regular", then "budget"
                        WHEN LOSSVIEW in (10,30,40) THEN 3 
                        WHEN LOSSVIEW IN (1, 3, 4) THEN 2 
                        WHEN LOSSVIEW IN (11, 33, 44) THEN 1 
                    END AS LOSSVIEWPRIORITY
                FROM 
                    REVO_morristown.DBO.LOSSANALYSIS
                WHERE
                    LOSSVIEW IN (1, 10, 11, 3, 30, 33, 4, 44)
                    and isactive = 1 
                    and isdeleted = 0 
                    and MODEL= 'AIR'
           ) V ON V.LOSSANALYSISID = A.LOSSANALYSISID
        WHERE 
            l.status in (4,10,21,22,23,31,27,32,33,36) 
            and segment in ('PC') 
            and A.ISACTIVE  = 1 
    )
    ,layerLossAnalyses_arc as (
        select layerid, lossanalysisid, LossViewGroup from layerLossAnalyses_arc_ranked where rankview = 1
	),
    layerYelt_arc as 
	(
		select 
            'ARC',
            la.LossAnalysisId, 
            la.LayerId, 
            EventId, 
            Peril, LossType, Year, Day, LossPct, RP, RB,
            LossViewGroup
		from 
            layerLossAnalyses_arc la
            inner join REVOLAYERLOSS_morristown.DBO.LAYERYELT Y on Y.LossAnalysisId = la.LossAnalysisId AND y.layerid = la.layerid
		where losstype = 1
	),
    merged as (
        select * from layeryelt_arl 
        union (select * from layeryelt_arc)
    )
    select
        m.*
    from 
        merged m
    where
        peril <> 'TR';