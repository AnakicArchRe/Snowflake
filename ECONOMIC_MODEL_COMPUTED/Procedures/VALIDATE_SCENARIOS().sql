CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.VALIDATE_SCENARIOS()
RETURNS TABLE (SCENARIOID NUMBER, PERIODID VARCHAR, RETROLEVELTYPE NUMBER, GROSS FLOAT, GROSSEXPLANATION VARCHAR)
LANGUAGE SQL
AS
$$
begin

    LET results RESULTSET := 
    (
        with 
            tmp_LayerPeriodCession_Scenario as (
                select
                    sc.scenarioid,
                    rb.retroblockid,
                    sum(rb.placement * case when r.isspecific =1 then ra.cessiongross else coalesce(rci.investmentsigned, rci.investmentsignedpctcalculated) end) as CessionGross
                from 
                    economic_model_staging.retrotag rb
                    inner join economic_model_staging.RetroConfiguration rpc on rpc.RetroConfigurationid = rb.RetroConfigurationid
                    inner join economic_model_staging.portlayerperiod lp on lp.periodid = rb.periodid
                    inner join economic_model_scenario.scenario sc
                    inner join economic_model_scenario.portlayer_scenario pl on pl.portlayerid = lp.portlayerid and pl.scenarioid = sc.scenarioid
                    inner join economic_model_scenario.retroinvestmentleg_scenario rci on rci.retroconfigurationid = rpc.retroconfigurationid and rci.scenarioid = sc.scenarioid
                    inner join economic_model_scenario.retrocontract_scenario r on rpc.retrocontractid = r.retrocontractid and r.scenarioid = sc.scenarioid
                    // left join because there will not be a retroallocation for projeted layers, this should only happen for retro with IsSpecific=1
                    left join economic_model_staging.retroallocation ra on ra.layerid = pl.layerid and ra.retrocontractinvestorid = rci.retrocontractinvestorid
                group by 
                    sc.scenarioid, rb.retroblockid
            )
            , cessionByLevelWithGross as (
                select
                    lpc_sc.scenarioid, periodid, 
                    -- allow overriding level for retro by scenario
                    -- scenario deals with levels (which are retrolevelype+1)
                    level,
                    -- we're zeroing out inactive retros when looking at their contribution to lower level retros. Scenarios can de-/activate retros if the user wants.
                    sum(case when rps.IsActive then lpc_sc.CessionGross else 0 end) as Gross, 
                    -- we're zeroing out inactive retros when looking at their contribution to lower level retros. Scenarios can de-/activate retros if the user wants.
                    listagg(
                        case 
                            when rps.IsActive then 
                                concat(case when HasOverride = 1 then concat('[[',rps.retrocontractid,']]') else rps.retrocontractid end, '(', trim(to_varchar(CessionGross * 100, '999.00')), '%)')
                            else ''--concat('Ignoring inactive retro ', rps.retrocontractid) 
                        end, 
                        '+'
                    ) GrossExplanation
                from
                    tmp_LayerPeriodCession_Scenario lpc_sc
                    inner join economic_model_staging.retrotag lpc on lpc.retroblockid = lpc_sc.retroblockid
                    inner join economic_model_staging.retroconfiguration rp_cfg on rp_cfg.retroconfigurationid = lpc.retroconfigurationid
                    left join economic_model_scenario.RetroContract_Scenario rps on rp_cfg.retrocontractid = rps.retrocontractid and lpc_sc.scenarioid = rps.scenarioid
                where periodid = 'ARL_877327:1-151' and lpc_sc.scenarioid = 1
                group by 
                    lpc_sc.scenarioid, periodid, level
            )
            select 
                *
            from 
                cessionByLevelWithGross
            where 
                // find block levels at which we have > 100% cession (less than 0 is available)
                gross > 1
    );

    return table(results);
end
$$;