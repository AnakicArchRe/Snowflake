create or replace view ACTUARIAL_ILS_POC.ECONOMIC_MODEL_SCENARIO.SCENARIO_PARTS(
	SCENARIOID,
	PARTID,
	HOPS,
	DEPTH
) as
        with recursive cte as (
            // each scenario consists at least of itself
            select 
                sf.scenarioid,
                sf.scenarioid as partId,
                0 as hops
            from 
                economic_model_scenario.scenario sf
            where 
                sf.isactive = 1
            
            union all
        
            // but also of any ancestors it might have
            select 
                sf.scenarioid,
                cte.partId,
                cte.hops + 1 as hops
            from 
                economic_model_scenario.scenario sf
                inner join cte on sf.parentscenarioid = cte.scenarioid
            where 
                sf.isactive = 1
        )
        select 
            *, 
            count(*) over (partition by scenarioid) - hops as depth 
        from 
            cte 
        order by 
            scenarioid, partid;