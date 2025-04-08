CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.POPULATE_RETROCONFIGURATIONS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
$$
BEGIN

    create or replace table economic_model_staging.retroconfiguration as
        with 
            sourcePriority as (
                select 
                    $1 as source, $2 as priority
                from values 
                    ('ARL',1), 
                    ('ARC',2)
            )
            , contractTerms as (
                SELECT DISTINCT
                    sp.priority as sourcePriority,
                    CONCAT(r.source_db, '_', R.RETROPROGRAMID) AS RetroProgramId,	
                    coalesce(K.STARTDATE, R.INCEPTION) AS StartDate,
                    coalesce(
                        nullifzero(K.TARGETCOLLATERAL), 
                        nullifzero(r.TGTINVESTORCOLL), 
                        nullifzero(I.TARGETCOLLATERAL)
                    ) TARGETCOLLATERAL,
                FROM 
                    economic_model_raw.RETROPROGRAM R	
                    inner join sourcePriority sp on sp.source = r.source_db
                    inner JOIN economic_model_raw.SPINSURER S ON S.RETROPROGRAMID = R.RETROPROGRAMID	and s.source_db = r.source_db
                    inner JOIN economic_model_raw.RETROINVESTOR I ON I.SPINSURERID = S.SPINSURERID	and i.isactive = 1 and i.source_db = r.source_db and i.targetcollateral is not null
                    LEFT JOIN economic_model_raw.RETROPROGRAMRESET K ON K.RETROPROGRAMID = R.RETROPROGRAMID and k.source_db = r.source_db and k.isactive = 1
                WHERE 
                    R.ISACTIVE = 1
                ORDER BY 
                    StartDate
            )
            , byContract as (
                SELECT 
                    M.RETROCONTRACTID, 
                    C.StartDate, 
                    -- We'll trust the highest priority source. It's possible that multiple investors for the same retro/date have a different tgtinvcoll. 
                    -- Example: ARL:1,31,55,58. In such cases, we pick the highest one. Todo: check with PC (is this an error in revo?).
                    RANK() OVER (PARTITION BY M.RETROCONTRACTID, StartDate ORDER BY M.RETROCONTRACTID, StartDate, C.sourcePriority, TARGETCOLLATERAL desc) AS SOURCERANK,
                    Max(TARGETCOLLATERAL) over (PARTITION BY M.RETROCONTRACTID, StartDate) as TARGETCOLLATERAL
                FROM 
                    economic_model_revoext.retroprogramcontractmapping M
                    INNER JOIN contractTerms C ON C.RetroProgramId = M.RETROPROGRAMID
            )
            SELECT 
                concat(RETROCONTRACTID, '[', rank() over (partition by RETROCONTRACTID order by StartDate asc), ']') as RetroConfigurationId,
                RETROCONTRACTID as RetroContractId, 
                StartDate, 
                TargetCollateral
            FROM 
                bycontract
            WHERE 
                SOURCERANK = 1
            ORDER BY 
                RETROCONTRACTID, StartDate;    
End
$$;