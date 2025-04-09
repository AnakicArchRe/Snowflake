CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.POPULATE_PORTLAYERCESSIONS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    -- work out whether a layer cedes to a retroprogram (not contract)...
    create or replace table economic_model_staging.PortLayerCession as
        with 
           cte as (
                select 
                    -- for each layer, we find the latest portfolio that has this layer and trust its cessions
                    -- note: must use p.sourceportfolioid (int) for ranking because p.portfolioId is a string and when comparing strings "999">"1000".
                    l.source_db, l.topupzoneid, pl.portlayerid, l.layerid, rank() over (partition by l.layerid order by p.portfolioid desc) rank
                from 
                    economic_model_raw.layer l
                    inner join economic_model_raw.portlayer pl on l.layerid = pl.layerid and pl.source_db = l.source_db
                    inner join economic_model_raw.portfolio p on pl.portfolioid = p.portfolioid and pl.source_db = p.source_db
                    -- todo: consider using a function for creating a combined id.
                    inner join economic_model_revoext.referenceportfolio rp on rp.portfolioid = concat(p.source_db, '_', p.portfolioid)
                    where 
                        p.jobstatus <> 0
                        and pl.isactive = 1 and pl.isdeleted = 0 
                        and l.isactive = 1 and l.isdeleted = 0 
                        and p.isactive = 1 and p.isdeleted = 0
            ) 
            -- ...continued
            , layercessionflags as (
                select distinct 
                    concat(cte.source_db, '_', cte.LayerId) as LayerId, 
                    concat(cte.source_db, '_', r.retroprogramid) RetroProgramid, 
                    coalesce(rz.cession, 1) as Placement
                from 
                    cte
                    inner join economic_model_raw.PortLayerCession plc on cte.portlayerid = plc.portlayerid and cte.source_db = plc.source_db
                    inner join economic_model_raw.RetroProgram r on plc.retroprogramid = r.retroprogramid and plc.source_db = r.source_db
                    left outer join economic_model_raw.RetroZone rz on r.retroprogramid = rz.retroprogramid and rz.topupzoneid = cte.topupzoneid and rz.source_db = cte.source_db
                where 
                    -- take the cessions from the latest portfolio with this layer
                    rank = 1 
                    and plc.isactive = 1 and plc.isdeleted = 0 and plc.cessiongross > 0 and plc.shouldcessionapply = 1
                    and r.isactive = 1 and r.isdeleted = 0 and r.status in (10,22,1,25)
            )
            -- expand to all portlayers (outside of reference portfolios) and extend with overlap start/end columns
            select
                concat(PortLayerId, '->', r.retroprogramid) as CessionId, 
                PortLayerId, 
                r.retroprogramid, 
                lc.Placement,
                case 
                    -- RAD (2) = entire layer is subject to retro
                    -- LOD (1) = only overlapping part is subject to retro
                    when r.retroprogramtype = 2 then pl.inception
                    when r.inception > pl.inception then r.inception 
                    else pl.inception 
                end as OverlapStart,
                case
                    -- RAD (2) = entire layer is subject to retro
                    -- LOD (1) = only overlapping part is subject to retro
                    when r.retroprogramtype = 2 then pl.expiration
                    when r.expiration < pl.expiration then r.expiration 
                    else pl.expiration 
                end as OverlapEnd
            from 
                layercessionflags lc
                inner join economic_model_staging.retroprogram r on lc.retroprogramid = r.retroprogramid
                inner join economic_model_staging.PortLayer pl on lc.layerid = pl.layerid
            where 
                -- ensure the portlayer and retro overlap
                pl.inception <= r.expiration and pl.expiration >= r.inception
                
                -- for RAD retros to apply, the layer must not start before the retro
                -- note: this is the reason we can't override retro type in scenarios
                -- at least at the moment. Otherwise, we'd have to move this calculation
                -- and subsequent ones into the post-scenario schema.
                and (r.retroprogramtype <> 2 or pl.inception >= r.inception)
                
                -- note: commented out because we can't limit to referenceportfolios because we want to 
                -- allow calculating gross-to-net results for all portfolios. We can introduce settings 
                -- to scenarios that limit what gets recalculated in each scenario.
                -- and p.isreferenceportfolio = 1
        ;
        
    return sqlrowcount;
    
end
;