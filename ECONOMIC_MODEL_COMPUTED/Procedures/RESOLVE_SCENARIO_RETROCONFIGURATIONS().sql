CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.RESOLVE_SCENARIO_RETROCONFIGURATIONS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    create or replace table economic_model_computed.retroconfiguration_scenario as
        select distinct
            sp.scenarioid, 
            l.retroconfigurationid,
            l.retrocontractid,
            l.startdate,
            l.targetcollateral as targetcollateralrevo,
            last_value(l_o.targetcollateraloverride) ignore nulls over (partition by sp.scenarioid, l.retroconfigurationid order by depth asc) targetcollateraloverride
        from
            economic_model_staging.retroconfiguration l
            cross join economic_model_scenario.scenario_parts sp
            left outer join economic_model_scenario.retroconfiguration_override l_o on sp.partid = l_o.scenarioid and l.retroconfigurationid = l_o.retroconfigurationid
            -- make sure the retro interacts with portfolios visible in the scenario
            inner join economic_model_computed.cessionflag f on l.retrocontractid = f.retrocontractid and sp.partid = f.scenarioid
        ;

END
;