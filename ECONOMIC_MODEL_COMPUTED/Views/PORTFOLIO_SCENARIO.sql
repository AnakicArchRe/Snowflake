create or replace view ACTUARIAL_ILS_POC.ECONOMIC_MODEL_COMPUTED.PORTFOLIO_SCENARIO(
	SCENARIOID,
	PORTFOLIOID,
	SOURCE,
	NAME,
	UWYEAR
) as
        select 
            scenarioid, p.*
        from 
            economic_model_scenario.scenario s
            cross join table(split_to_table(coalesce(s.analysis_PORTFOLIOIDS, ''), ',')) t
            inner join economic_model_staging.portfolio p on trim(t.value) = p.portfolioid
    ;