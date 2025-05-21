CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.RESOLVE_SCENARIO_PORTLAYERS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    create or replace table economic_model_computed.portLayer_scenario as
        with withResolvedAndOriginalValues as (
            select distinct
                sp.scenarioid, 
                pl.* exclude (sharefactor, premiumfactor),
                -- find closest (deepest) sharefactor/premiumfactor override, either on the portlayer itself or its topupzone.
                case 
                    -- note: cannot override inforce portlayers
                    -- todo: check other entities and verify this is the case
                    when pl.layerview = 'INFORCE' then pl.sharefactor 
                    else coalesce(last_value(coalesce(pl_o.sharefactor, tz_u_o.sharefactor)) ignore nulls over (partition by sp.scenarioid, pl.portlayerid order by depth asc), pl.sharefactor)
                end sharefactor,
                case 
                    when pl.layerview = 'INFORCE' then pl.premiumfactor 
                    else coalesce(last_value(coalesce(pl_o.premiumfactor, tz_u_o.premiumfactor)) ignore nulls over (partition by sp.scenarioid, pl.portlayerid order by depth asc), pl.premiumfactor) 
                end premiumfactor,
                pl.sharefactor as shareFactor_original,
                pl.premiumfactor as premiumFactor_original,
            from
                economic_model_staging.portlayer pl
                cross join economic_model_scenario.scenario_parts sp
                -- limit to portfolios included in scenario
                inner join economic_model_computed.portfolio_scenario pf on pl.portfolioid = pf.portfolioid and pf.scenarioid = sp.partid
                left outer join economic_model_scenario.portlayer_override pl_o on sp.partid = pl_o.scenarioid and pl.portlayerid = pl_o.portlayerid
                left outer join economic_model_computed.topupzone_override_unpivoted tz_u_o on sp.partid = tz_u_o.scenarioid and pl.topupzoneid = tz_u_o.topupzoneid and upper(tz_u_o.productgroup) = upper(pl.productgroup)
        )
        select 
            x.* exclude (shareFactor_original, premiumFactor_original, limit100pct, premium100pct, boundfxdate),
            -- note: if the scenario attempts to lock in the boundfx, then use the boundfx of the layer if available (for inforce) and use
            -- the scenario fxdate as a fallback (for projected). If the scenario doesn't lock in boundfx, use scenario.fxdate for all layers.
            limit100pct * fx.rate as limit100Pct,
            premium100pct * fx.rate as premium100pct,
            iff(sc.boundfxlockin, coalesce(x.boundFxDate, sc.fxdate), sc.fxdate) used_fx_date,
            fx.rate as used_fx_rate,
            s.currency as original_currency,
            limit100pct as limit100pct_original_currency, 
            premium100pct as premium100pct_original_currency,
            economic_model_computed.concat_non_null(
                economic_model_computed.compare_and_note(sharefactor, sharefactor_original, 'ShareFactor'),
                economic_model_computed.compare_and_note(premiumfactor, premiumfactor_original, 'PremiumFactor')
            ) AS notes
        from 
            withResolvedAndOriginalValues x
            inner join economic_model_scenario.scenario sc on x.scenarioid = sc.scenarioid
            inner join economic_model_staging.submission s on x.submissionid = s.submissionid
            inner join economic_model_staging.fxrate fx on s.currency = fx.currency and used_fx_date = fx.fxdate and fx.basecurrency = 'USD'
        ;

END
;