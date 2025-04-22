CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.POPULATE_RETROTAGS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
BEGIN

    -- Tag each period with a list of retroconfigurations it cedes to.
    create or replace table economic_model_staging.RetroTag as
        with 
            periodRetros as (
                select
                    p.portlayerid,
                    p.periodid, 
                    p.periodstart,
                    plc.retroprogramid, 
                    plc.placement, 
                    
                    -- -- RAD retros use the configuration that was active when the layer began and ignore subsequent resets
                    -- -- while for LOD retros each period is affected by the latest retro configuration.
                    -- case when retroprogramtype = 2 then plc.overlapstart else p.periodstart end configDate,
                    
                    -- commented out above because we now use the point of first interaction for both RAD and LOD 
                    -- when determining which retroconfiguration applies to a block.
                    plc.overlapstart as configDate,
                from 
                    economic_model_staging.PortLayerPeriod p
                    -- find active retro contracts for each period
                    inner join economic_model_staging.portlayercession plc on 
                        p.portlayerid = plc.portlayerid
                        and plc.overlapstart <= p.periodend
                        and plc.overlapend >= p.periodstart
            )
            -- map retroprograms tags to retrocontract (retroprogram and retrocontracts have a many to many relationship)
            , byContract as (
                select 
                    -- distinct because we can have more than one retroprogram in a retrocontract (but they all have the same placement on the same config date)
                    distinct pr.portlayerid, pr.periodid, periodstart, m.retrocontractid, pr.placement, pr.configdate
                from 
                    periodretros pr
                    inner join economic_model_revoext.retroprogramcontractmapping m on pr.retroprogramid = m.retroprogramid
            )
            , withFirstInteractionPeriod as (
                select 
                    *,
                    -- note: this column can be used by the economic_model_computed.process_model stored procedure if we want to make sure 
                    -- all blocks of a cession use the same parameters that were present when the cession started (e.g. cession is 
                    -- unaffected by new retros starting or lower-level retros ending). At the time of writing this comment, this is 
                    -- not the case, but I've put this in place so it's easy to switch to this method of calculation.
                    first_value(periodId) over(partition by portlayerid, retrocontractid order by periodstart asc) as cessionstartperiodid
                from 
                    byContract
            )
            -- find the configuration applicable to the block...
            , withConfigsRanked as (
                select 
                    pr.*, 
                    retroconfigurationid, 
                    -- take the newest configuration on or before the startdate for the period
                    rank() over (partition by periodid, cfg.retrocontractid order by cfg.startdate desc) rank
                from 
                    withFirstInteractionPeriod pr
                    // find all configs on or before the config date
                    left join economic_model_staging.retroconfiguration cfg on 
                        pr.retrocontractid = cfg.retrocontractid 
                        and cfg.startdate <= pr.configDate
            )
            -- ...continued
            ,withConfigs as (
                select 
                    concat(periodid, '->', RetroConfigurationid) as RetroBlockId, 
                    periodid,
                    cessionstartperiodid,
                    RetroConfigurationid, 
                    placement
                from 
                    withConfigsRanked 
                where 
                    rank = 1
            )
            select 
                *
            from
                withConfigs
                ;
end
;