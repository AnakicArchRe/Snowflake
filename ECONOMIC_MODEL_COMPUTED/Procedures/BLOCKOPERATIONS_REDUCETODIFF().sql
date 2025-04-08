CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.BLOCKOPERATIONS_REDUCETODIFF()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
$$
begin

    truncate table economic_model_computed.blockoperations_out;

    insert into economic_model_computed.blockoperations_out (scenarioid, blockid, exposedlimit, exposedrp, premiumprorata, expensesprorata, exposedpremium, exposedexpenses)
        with scenarioAndBlockIds as (
            select distinct blockid, b.scenarioid, parentscenarioid
            from economic_model_computed.blockoperations_in b
            inner join economic_model_scenario.scenario sc on sc.scenarioid = b.scenarioid
        ), res as (
        select 
            sb.scenarioid,
            sb.blockid,
            zeroifnull(b1.exposedlimit) - zeroifnull(b2.exposedlimit) as exposedlimit,
            zeroifnull(b1.exposedrp) - zeroifnull(b2.exposedrp) as exposedrp,
            zeroifnull(b1.premiumprorata) - zeroifnull(b2.premiumprorata) as premiumprorata,
            zeroifnull(b1.expensesprorata) - zeroifnull(b2.expensesprorata) as expensesprorata,
            zeroifnull(b1.exposedpremium) - zeroifnull(b2.exposedpremium) as exposedpremium,
            zeroifnull(b1.exposedexpenses) - zeroifnull(b2.exposedexpenses) as exposedexpenses,
        from 
            scenarioAndBlockIds sb
            left outer join economic_model_computed.blockoperations_in b1 on sb.scenarioid = b1.scenarioid and b1.blockid = sb.blockid
            left outer join economic_model_computed.blockoperations_in b2 on sb.parentscenarioid = b2.scenarioid and b2.blockid = sb.blockid
        )
        select
            *
        from
            res
        where
            abs(exposedlimit) > 0 or
            abs(exposedrp) > 0 or
            abs(premiumprorata) > 0 or
            abs(expensesprorata) > 0
    ;

    truncate table economic_model_computed.blockoperations_in;
end
$$;