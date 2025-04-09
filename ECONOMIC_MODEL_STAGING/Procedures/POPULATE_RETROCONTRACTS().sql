CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.POPULATE_RETROCONTRACTS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    -- 1. housekeeping (clean up old/orphaned entries) - delete contracts with no retroprograms (except catchall placeholder contracts i.e. net position)
    delete 
        from economic_model_revoext.retrocontract r
    where 
        not exists (
            select * from economic_model_revoext.retroprogramcontractmapping m where m.retrocontractid = r.retrocontractid
        ) 
        and level < 10;

    
    begin transaction;

    -- Add missing mappings and contracts
    -- 2.a. When a new retroprogram arrives from REVO, add a mapping for it
    insert into economic_model_revoext.retroprogramcontractmapping(retroprogramid, retrocontractid)
        select
            r.retroprogramid, r.retroprogramid
        from
            economic_model_staging.retroprogram r
            where not exists (select * from economic_model_revoext.retroprogramcontractmapping m where m.retroprogramid = r.retroprogramid);

    -- 2.b. create retrocontract for any mapping that doesn't refer to an existing contract
    -- note: we populate columns not present in REVO with default values that the user can adjust from the scenario editor, if needed
    insert into economic_model_revoext.retrocontract(retrocontractid, nonmodeledload, climateload, capitalcalculationlossview, capitalcalculationtargetreturnperiod)
        -- for newly formed contracts (retroprogram was placed in a new contract in the mapping table in the scenario editor)
        select
            distinct retrocontractid, 1, 1, 'ARCH', 4
        from
            economic_model_revoext.retroprogramcontractmapping m
            where not exists (select * from economic_model_revoext.retrocontract r where m.retrocontractid = r.retrocontractid);
    
    -- 3. update retrocontract information based on member retroprograms
    create or replace temporary table economic_model_staging.retrocontractsettings as
        with sourcePriority as (
            select
                $1 as source,
                $2 as priority
            from values
                ('ARL', 1),
                ('ARC', 2)
        ), groupedAndRanked as (
            select 
                m.retrocontractid,
                -- first member inception (used to determine exposureStart, but only as a cap)
                min(inception) over (partition by m.retrocontractid) minInception,
                -- exposureend is last memer's expiration
                max(expiration) over (partition by m.retrocontractid) as maxExpiration,
                -- exposure start is -1year+1day relative to maxExpiration, capped by MinInception
                greatest(dateadd(day, 1, dateadd(year, -1, maxExpiration)), minInception) as exposureStart,
                -- other information is read from the primary retroprogram in the contract (latest one from the highest priority source)
                r.*,
                rank() over (partition by m.retrocontractid order by sp.priority asc, r.inception desc /*question for PC: should the last one by inception be relevant, or should it be last one by created date?*/) as rank
            from 
                economic_model_staging.retroprogram r
                inner join economic_model_revoext.retroprogramcontractmapping m on r.retroprogramid = m.retroprogramid
                inner join sourcepriority sp on r.source = sp.source
        ), byRetroContract as (
            select * from groupedandranked
            where rank = 1
        )
        select * from byRetroContract
        order by len(retrocontractid) desc
        ;

    -- update retrocontracts with data from REVO
    -- note: consider inlining the temp table above as a subquery in below update command
    update 
        economic_model_revoext.retrocontract r
    set 
        r.name = s.name,
        -- for inception of the contract we use the earliest inception date
        r.inception = mininception,
        -- for expiration of the contract we use the latest expiration date
        r.expiration = maxExpiration,
        r.retroprogramtype = s.retroprogramtype,
        r.level = s.level,
        r.exposurestart = s.exposurestart,
        r.exposureend = s.maxExpiration,
        r.commissiononnetpremium = s.commissiononnetpremium,
        r.profitcommissionpctofprofit = s.profitcommission,
        r.reinsuranceexpensesoncededcapital = s.reinsuranceexpensesoncededcapital,
        r.reinsuranceexpensesoncededpremium = s.reinsuranceexpensesoncededpremium,
        r.reinsurancebrokerageonnetpremium = s.reinsurancebrokerageonnetpremium,
        r.isactive = s.isactive,
        r.isspecific = s.isspecific    
    from 
        economic_model_staging.retrocontractsettings s
    where 
        r.retrocontractid = s.retrocontractid;

    commit;
end
;