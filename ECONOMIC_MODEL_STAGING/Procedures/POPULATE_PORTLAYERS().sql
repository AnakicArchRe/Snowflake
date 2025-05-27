CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.POPULATE_PORTLAYERS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
BEGIN
    
    -- portlayers
    create or replace table ECONOMIC_MODEL_STAGING.PortLayer as
        WITH ELINPUT AS (
            SELECT
                Source_db,
                LAYERID,
                RANK() OVER ( PARTITION BY source_db, LAYERID ORDER BY LAYERID, R.LOSSVIEW DESC) AS RANKVIEW,
                R.EL
            FROM
                economic_model_raw.lossviewresult R
            WHERE
                R.LOSSVIEW IN (1, 10) 
                AND R.ISACTIVE = 1
        )
        , PRODUCTGROUP AS (
            SELECT 
                e.source_db,
                l.LAYERID, 
                EL,
                CASE 
                    WHEN (COALESCE(e.EL,0) >= 0.2 AND L.FACILITY IN ('9PC', '5PC') AND cp.LEGALENTCODE IN ('ARL', 'ARE')) THEN 'Linteau' 
                    WHEN COALESCE(e.EL,0) <= 0.05 THEN 'TopUp' 
                    ELSE 'Middle' 
                END AS PRODUCTGROUP
            FROM 
                ELINPUT e
                inner join economic_model_raw.layer l on e.layerid = l.layerid and e.source_db = l.source_db
                inner join economic_model_raw.submission s on l.submissionid = s.submissionid and l.source_db = s.source_db
                inner join economic_model_raw.program p on p.programid = s.programid and p.source_db = s.source_db
                inner join economic_model_raw.cedent c on p.cedentid = c.cedentid and c.source_db = p.source_db
                inner join economic_model_raw.cedentgroup cg on c.cedentgroupid = cg.cedentgroupid and c.source_db = cg.source_db
                inner join economic_model_raw.company cp on p.companyid = cp.companyid and p.source_db = cp.source_db
            WHERE
                RANKVIEW = 1
        )         
        , layer as (
            select 
                concat(l.Source_db, '_', l.LayerId) as LayerId, 
                l.Source_db as Source,
                concat(l.Source_db, '_', l.Submissionid) as Submissionid, 
                Placement, ReinstCount, 
                Inception, Expiration, Facility, Segment, LOB, 
                limitbasis,
                agglimit,
                occlimit,
                risklimit,
                signedshare as diag_signedshare, 
                estimatedshare as diag_estimatedshare, 
                authshare as diag_authshare, 
                quotedcorreshare as diag_quotedcorreshare,
                case 
                    when signedshare > 0 then signedshare
                    when estimatedshare > 0 then estimatedshare
                    else budgetshare
                end as Share,
                l.Status, 
                LayerDesc, 
                Commission + CommOverride + Brokerage + Tax AS EXPENSES, 
                case 
                    when premium > 0 then premium
                    else budgetpremium
                end as Premium,
                TopUpZoneId,
                case 
                    when l.limitbasis = 1 then l.agglimit
                    when l.limitbasis in (4,7) then l.risklimit
                    else l.occlimit
                end as limit100Pct,
                case 
                    when l.placement = 0 then 0 
                    else l.premium / l.placement 
                end as Premium100Pct,
                coalesce(productgroup, 'UNKNOWN') as ProductGroup,
                -- must use to_date to strip time as some old entries have time in there
                to_date(l.boundfxdate) as boundfxdate,
                to_date(s.fxdate) as submission_fxdate,
                EL
            from 
                economic_model_raw.layer l
                inner join economic_model_raw.submission s on l.submissionid = s.submissionid and l.source_db = s.source_db
                inner join productgroup pg on l.layerid = pg.layerid AND L.SOURCE_DB = PG.SOURCE_DB
            where 
                l.isactive = 1 and l.isdeleted = 0 
                and s.isactive = 1 and s.isdeleted = 0
        )
        , PortLayerData as (
            select
                concat(pl.source_db, '_', pl.PortLayerId) as PortLayerId, 
                concat(pl.source_db, '_', pl.LayerId) as LayerId, 
                concat(pl.source_db, '_', pl.PortfolioId) as PortfolioId, 
                CAST(DATE_FROM_PARTS(p.UWYEAR,1,1) AS TIMESTAMP_NTZ(7)) AS PORTFOLIOSTART,
                CAST(DATE_FROM_PARTS(p.UWYEAR,12,31) AS TIMESTAMP_NTZ(7)) AS PORTFOLIOEND,
                pl.source_db as Source,
                CASE
                    WHEN l.EXPIRATION >= PORTFOLIOEND THEN 'INFORCE' /* IF A LAYER EXPIRES AFTER THEN END OF PORTFOLIO, THEN THE LAYER WILL NOT BE RENEWED IN THIS PORTFOLIO E.G. PORTFOLIO 1037, LAYERID 139559, PORTLAYERID 869408 */
                    WHEN (l.EXPIRATION < P.ASOFDATE AND l.INCEPTION < PORTFOLIOSTART ) THEN 'IGNORE' /* SUCH LAYER WOULD BE A MISTAKE FROM THE PORTFOLIO MODULE E.G. PORTFOLIO 1037, LAYERID 129272, PORTLAYERID 868905 */
                    WHEN (l.EXPIRATION < P.ASOFDATE ) THEN 'INFORCE' /* SUCH LAYER WOULD BE A SHORT TERM LAYER THAT IS NO LONGER INFORCE BUT INCLUDED IN THE PORTFOLIO TO HAVE A COMPLETE VIEW OF THE UNDERWRITING YEAR E.G. PORTFOLIO 1037, LAYERID 146942, PORTLAYERID 869835 */
                    WHEN  P.PORTFOLIOTYPE  = 0 THEN 'INFORCE'
                    WHEN  P.PORTFOLIOTYPE  = 1 THEN 'PROJECTION1'
                    WHEN (P.PORTFOLIOTYPE  = 2 AND YEAR(l.INCEPTION) = YEAR(P.ASOFDATE)) THEN 'INFORCE'
                    WHEN (P.PORTFOLIOTYPE  = 2 AND YEAR(l.INCEPTION) = YEAR(P.ASOFDATE)) THEN 'INFORCE'
                    WHEN (P.PORTFOLIOTYPE  = 2 AND YEAR(l.INCEPTION) = YEAR(P.ASOFDATE) - 1) THEN 'PROJECTION1'
                    WHEN (P.PORTFOLIOTYPE  = 2 AND YEAR(l.EXPIRATION) = YEAR(P.ASOFDATE)) THEN 'PROJECTION1'
                    WHEN (P.PORTFOLIOTYPE  = 3 AND YEAR(l.INCEPTION) = YEAR(P.ASOFDATE)) THEN 'PROJECTION1'
                    WHEN (P.PORTFOLIOTYPE  = 3 AND YEAR(l.INCEPTION) = YEAR(P.ASOFDATE) - 1) THEN 'PROJECTION2'
                    WHEN (P.PORTFOLIOTYPE  = 3 AND YEAR(l.EXPIRATION) = YEAR(P.ASOFDATE)) THEN 'PROJECTION2'
                    ELSE 'NOTINCLUDED'
                END AS LAYERVIEW,
                CASE
                    WHEN LAYERVIEW = 'INFORCE' THEN L.INCEPTION
                    WHEN l.EXPIRATION < p.ASOFDATE THEN L.INCEPTION /* THESE LAYERS SHOULD CODED AS INFORCE AND THUS HANDLED BY FIRST WHEN STATEMENT */
                    WHEN l.EXPIRATION >= PORTFOLIOEND THEN L.INCEPTION /* LAYERS THAT WILL EXPIRE AFTER THE PORTFOLIO WILL NOT BE RENEWED AS PART OF THIS PORTFOLIO3 CHECK THAT THIER STATUS IS INFORCE */
                    WHEN LAYERVIEW = 'PROJECTION1' THEN DATEADD(DAY, 1, L.EXPIRATION)
                    WHEN LAYERVIEW = 'PROJECTION2' THEN DATEADD(YEAR,1,DATEADD(DAY, 1, L.EXPIRATION))
                    ELSE '1900-01-01'        
                END AS Inception, 
                CASE
                    WHEN LAYERVIEW = 'INFORCE' THEN L.EXPIRATION  
                    WHEN L.EXPIRATION < p.ASOFDATE THEN L.EXPIRATION   /* THESE LAYERS SHOULD CODED AS INFORCE AND THUS HANDLED BY FIRST WHEN STATEMENT */
                    WHEN L.EXPIRATION >= PORTFOLIOEND THEN L.EXPIRATION   /* LAYERS THAT WILL EXPIRE AFTER THE PORTFOLIO WILL NOT BE RENEWED AS PART OF THIS PORTFOLIO3 CHECK THAT THIER STATUS IS INFORCE */
                    WHEN LAYERVIEW = 'PROJECTION1' THEN DATEADD(YEAR, 1, L.EXPIRATION)
                    WHEN LAYERVIEW = 'PROJECTION2' THEN DATEADD(YEAR, 2, L.EXPIRATION)
                    ELSE '1900-01-01'    
                END AS Expiration,
                Share, ShareAdjusted, Share2Adjusted, pl.Premium, PremiumAdjusted, Premium2Adjusted,
            from 
                economic_model_raw.portlayer pl
                inner join economic_model_raw.layer l on pl.layerid = l.layerid and l.isdeleted = 0 and l.isactive = 1 and l.source_db = pl.source_db
                inner join economic_model_raw.portfolio p on pl.portfolioid = p.portfolioid and p.isactive = 1 and p.isdeleted = 0 and pl.source_db = p.source_db
            where 
                pl.isactive = 1 and pl.isdeleted = 0
        )
        select
            pld.Source,
            PortLayerId,
            pld.PortfolioId, 
            l.LayerId,
            LayerView,
            case when pld.layerview = 'INFORCE' then coalesce(boundfxdate, submission_fxdate) else null end as boundFxDate,
            iff(boundfxdate is not null, 'Layer', 'Submission') as boundFxDate_source,
            pld.inception, 
            pld.expiration,
            l.inception as OriginalLayerInception,
            l.expiration as OriginalLayerExpiration,
            l.submissionid,
            l.reinstcount,
            l.facility,
            l.segment,
            l.lob,
            l.share,
            l.diag_signedshare, 
            l.diag_estimatedshare, 
            l.diag_authshare, 
            l.diag_quotedcorreshare,
            l.layerdesc,
            l.expenses,
            l.topupzoneid,
            l.limit100pct,
            l.premium100pct,
            l.productgroup,
            l.el,
            l.status,
    		case 
                when pld.Share is null or pld.Share = 0 then 1
                when LayerView = 'INFORCE' then 1
                when LayerView = 'PROJECTION1' then ShareAdjusted / pld.Share
                when LayerView = 'PROJECTION2' then Share2Adjusted / pld.Share
            end as ShareFactor,
            case 
                when pld.Premium is null or pld.Premium = 0 then 1
                when LayerView = 'INFORCE' then 1
                when LayerView = 'PROJECTION1' then PremiumAdjusted / pld.Premium
                when LayerView = 'PROJECTION2' then Premium2Adjusted / pld.Premium
            end as PremiumFactor,
            l.limitbasis,
            l.agglimit,
            l.risklimit,
            l.occlimit,
            l.placement,
            l.premium
        from 
            portlayerdata pld
            inner join layer l on pld.layerid = l.layerid
        where 
            LayerView <> 'NOTINCLUDED' 
    ;
End
;