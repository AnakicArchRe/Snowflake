CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.BLOCKOPERATIONS_EXPANDFROMDIFF()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
$$
begin
    
    truncate table economic_model_computed.blockoperations_out;

    insert into economic_model_computed.blockoperations_out (scenarioid, blockid, exposedlimit, exposedrp, premiumprorata, expensesprorata, exposedpremium, exposedexpenses)
        select 
            sp.scenarioid,
            b.blockid,
            sum(exposedlimit) as exposedlimit,
            sum(exposedrp) as exposedrp,
            sum(premiumprorata) as premiumprorata,
            sum(expensesprorata) as expensesprorata,
            sum(exposedpremium) as exposedpremium,
            sum(exposedexpenses) as exposedexpenses
        from 
            economic_model_scenario.scenario_parts sp
            inner join economic_model_computed.blockoperations_in b on sp.partid = b.scenarioid
        group by
            sp.scenarioid, b.blockid
    ;

    truncate table economic_model_computed.blockoperations_in;
end
$$;