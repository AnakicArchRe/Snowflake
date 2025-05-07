create or replace view ACTUARIAL_ILS_POC.ECONOMIC_MODEL_COMPUTED.CESSIONFLAG(
	SCENARIOID,
	RETROCONTRACTID
) as 
        select distinct
            p.scenarioid, retrocontractid
        from 
            economic_model_computed.portfolio_scenario p
            inner join economic_model_staging.portlayer pl on p.portfolioid = pl.portfolioid
            inner join economic_model_staging.portlayerperiod per on pl.portlayerid = per.portlayerid
            inner join economic_model_staging.retrotag t on per.periodid = t.periodid
            inner join economic_model_staging.retroconfiguration rc on t.retroconfigurationid = rc.retroconfigurationid;