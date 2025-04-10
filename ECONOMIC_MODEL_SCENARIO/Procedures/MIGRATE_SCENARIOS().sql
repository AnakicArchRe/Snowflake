CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_SCENARIO.MIGRATE_SCENARIOS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    -- 1. scenario table
    delete from ACTUARIAL_ILS_POC.ECONOMIC_MODEL_SCENARIO.SCENARIO 
    -- keep our scenario 1 because it has additional data that the old one doesn't
    where scenarioid <> 1;
    
    insert into economic_model_scenario.scenario (
        SCENARIOID,
    	NAME,
    	DESCRIPTION,
    	ISACTIVE,
    	SORTORDER,
        ParentScenarioId,
    	ANALYSIS_PORTFOLIOIDS,
    	ANALYSIS_RETROCONTRACTIDS
    )
        with retroProgramToContractMap as (
            select 
                trim(value, '""') as retroprogramid, 
                rc.retrocontractid
            from 
                economic_model_revoext.retrocontract rc,
                lateral flatten(regexp_substr_all(rc.retrocontractid, 'AR[CL]_\\d*'))
        )
        , scenarioIncludedRetroPrograms as (
            select 
                s.scenarioid, r.value as retroProgramId
            from 
                economic_model.scenario s, 
                table(split_to_table(s.analysis_retroprogramids, ',')) r
        ), withContractIdsForAnalysis as (
            select 
                r.scenarioid, 
                listagg(r.retroprogramid, ',') within group (order by r.retroprogramid) as retroprograms, 
                listagg(retrocontractid, ',') within group (order by r.retroprogramid) as retrocontractids
            from 
                scenarioIncludedRetroPrograms r
                inner join retroProgramToContractMap m on r.retroprogramid = m.retroprogramid
                group by r.scenarioid
        )
        select 
            s.scenarioid, 
            s.name, 
            s.description,
            s.isactive,
            s.sortorder,
            case when s.scenarioid = 1 then null else 1 end,
            s.analysis_portfolioids,
            m.retrocontractids as analysis_retrocontractids
        from 
            economic_model.scenario s
            left join withContractIdsForAnalysis m on s.scenarioid = m.scenarioid
        where s.scenarioid <> 1;



    
    -- 2. portlayer overrides
    create or replace table economic_model_scenario.portlayer_override as
        select 
            pl_s.scenarioid, 
            pl_s.portlayerid, 
            pl_s.sharefactoroverride as sharefactor, 
            pl_s.premiumfactoroverride as premiumfactor
        from 
            economic_model.portlayer_scenario pl_s
            inner join economic_model_staging.portlayer pl on pl_s.portlayerid = pl.portlayerid
        where
            pl.layerview = 'INFORCE'
            and sharefactoroverride  <> pl.sharefactor or premiumfactoroverride <> pl.premiumfactor;

            
    -- 3. 
    
    truncate economic_model_scenario.retrocontract_override;
    insert into economic_model_scenario.retrocontract_override(scenarioid, retrocontractid, level, isactive)
        with retroProgramToContractMap as (
            select 
                trim(value, '""') as retroprogramid, 
                rc.* 
            from 
                economic_model_revoext.retrocontract rc,
                lateral flatten(regexp_substr_all(rc.retrocontractid, 'AR[CL]_\\d*'))
        )
        select distinct 
            ov.scenarioid, 
            retrocontractid, 
            ov.level, 
            ov.isactive 
        from 
            retroprogramtocontractmap m
            inner join economic_model.retroprogram_scenario ov on m.retroprogramid = ov.retroprogramid;
    
    -- 4. retroprogramconfiguration_scenario
    truncate economic_model_scenario.retroconfiguration_override;
    insert into  economic_model_scenario.retroconfiguration_override(retroconfigurationid, scenarioid, targetcollateraloverride)
        with 
            cte as (
                select 
                    trim(regexp_substr(retroprogramconfigurationid, '\\[.*\\]'), '[]') as resetId, 
                    regexp_substr(retroprogramconfigurationid, '^AR[CL]_\\d*') as retroprogramid,
                    left(retroprogramid, 3) as sourceDb,
                    case when resetId = 'initial' then null else cast(right(resetid, length(resetid)-4) as int) end as resetIdRaw,
                    cast(right(retroprogramid, length(retroprogramid)-4) as int) as retroProgramIdRaw,
                    scenarioid,
                    targetcollateraloverride
                from 
                    economic_model.retroprogramconfiguration_scenario
            )
            , retroProgramToContractMap as (
                select 
                    trim(value, '""') as retroprogramid, 
                    rc.* 
                from 
                    economic_model_revoext.retrocontract rc,
                    lateral flatten(regexp_substr_all(rc.retrocontractid, 'AR[CL]_\\d*'))
            )
            , ext as (
                select 
                    m.retrocontractid,
                    cte.sourcedb,
                    retroprogramidraw, 
                    resetId,
                    resetIdRaw, 
                    scenarioid,
                    case when rr.startdate is null then r.inception else rr.startdate end as startdate,
                    targetcollateraloverride
                from 
                    cte
                    inner join economic_model_raw.retroprogram r on r.source_db = sourcedb and r.retroprogramid = cte.retroprogramidraw
                    left join economic_model_raw.retroprogramreset rr on rr.source_db = sourcedb and rr.retroprogramresetid = resetidraw  
                    left join retroProgramToContractMap m on cte.retroprogramid = m.retroprogramid
            )
            , withCofiguration as (
                select ext.*, retroconfigurationid from ext
                left join economic_model_staging.retroconfiguration rc on rc.retrocontractid = ext.retrocontractid and ext.startdate = rc.startdate
            )
            select distinct
                retroconfigurationid, scenarioid, targetcollateraloverride 
            from 
                withCofiguration
            // todo: debug - invert below criteria, use * in select list to find programs for which no contract/configuration exists
            where
                retroconfigurationid is not null;
    
    
    -- 5. retroinvestmentleg override
    truncate economic_model_scenario.retroinvestmentleg_override;
    insert into economic_model_scenario.retroinvestmentleg_override(retroinvestmentlegid, scenarioid, investmentsignedpctentry, investmentsignedpct, investmentsignedamt)
        with 
            sourcePriority as (
                select 
                    $1 as source,
                    $2 as priority
                from values
                    ('ARL', 1), -- (lower is stronger)
                    ('ARC', 2)
            ),
            extractedIds as (
                select 
                    *, 
                    regexp_substr(retroprogramconfigurationinvestorid, '->.*') x,
                    right(x, length(x) - 2) as RetroInvestorId,
                    right(left(x, 5),3) as source,
                    left(retroprogramconfigurationinvestorid, length(retroprogramconfigurationinvestorid) - length(x)) as RetroProgramConfigurationId
                from 
                    economic_model.retroprogramconfigurationinvestor_scenario
            )
            , ext as (
                select 
                    x.*, rc.retroprogramid, rc.date, sp.priority
                from 
                    extractedids x
                    left join economic_model.retroprogramconfiguration rc on x.RetroProgramConfigurationId = rc.retroprogramconfigurationid
                    inner join sourcePriority sp on x.source = sp.source
            )
            , programToContractMapping as (
                select 
                    trim(value, '""') as retroprogramid, 
                    rc.* 
                from 
                    economic_model_revoext.retrocontract rc,
                    lateral flatten(regexp_substr_all(rc.retrocontractid, 'AR[CL]_\\d*'))
            )
            , investorIdMapping as (
                select distinct
                    value as retroInvestorId, rci.retrocontractinvestorid, rci. retrocontractid
                from 
                    economic_model_staging.retrocontractinvestor rci
                    -- Note: I didn't know how better to do the join since each investor group only gets an id after grouping
                    -- and I couldn't figure out an easier way to map back to retro investor ids. This isn't ideal since it will
                    -- break if we change the formatting in the retroinvestorIds column.
                    , table(split_to_table(rci.retroinvestorIds, '&'))
            )
            select distinct
                ril.retroinvestmentlegid,
                scenarioid, 
                min_by(investmentsignedpct_override, priority), 
                min_by(investmentsignedoverride, priority),
                min_by(investmentsignedamt_override, priority)
            from 
                ext x
                left join investorIdMapping im on x.retroinvestorid = im.retroinvestorid
                left join programToContractMapping pm on pm.retroprogramid = x.retroprogramid
                left join economic_model_staging.retroconfiguration rc on rc.retrocontractid = pm.retrocontractid and rc.startdate = x.date
                left join economic_model_staging.retroinvestmentleg ril on ril.retroconfigurationid = rc.retroconfigurationid and ril.retrocontractinvestorid = im.retrocontractinvestorid
                -- todo: invert this criteria and debug (why we have retroinvestors that we can't map to retrocontractinvestors)
            where 
                im.retroinvestorid is not null
            group by 
                ril.retroinvestmentlegid,
                scenarioid;
    
    -- 6. 
    -- select * from economic_model_scenario.topupzone_override
    
    -- currently empty, might not need it
    -- select * from economic_model.topupzone_scenario

    --7. reference porfolios


    create or replace table economic_model_revoext.referenceportfolio as 
    select * from economic_model.referenceportfolio;
end
;