CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.POPULATE_RETROINVESTOR_DATA()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
$$
BEGIN

    create or replace temp table retroinvestormappingdata as
        with sourcePriority as (
            -- we will only use investor data from the highest priority source for a given retrocontract configuration
            select $1 source_db, $2 priority
            from values
            ('ARL', 1),
            ('ARC', 2)
        ),
        ranked as (
            select
                concat(r.source_db, '_', i.retroinvestorid) retroinvestorid,
                r.source_db,
                i.name,
                rgm.retrocontractid,
                segregatedaccount,
                trim(c.name) as reinsurername,
                coalesce(k.startdate, r.inception) as configdate,
                -- -- note: I'm replacing the commented out lines with subsequent lines because it looks like we can have
                -- a 0 in the reset row, and a non-zero value in the investor row (different values even for the initial data). This is likely
                -- a bug in REVO. In Teams chat with PC (2024-12-30) concluded that the data from the reset are to be trusted more.
                -- coalesce (nullifzero(k.investmentsigned), nullifzero(i.investmentsigned)) as investmentsigned,
                -- coalesce (nullifzero(k.investmentsignedamt), nullifzero(i.investmentsignedamt)) as investmentsignedamt,
                coalesce (k.investmentsigned, nullifzero(i.investmentsigned)) as investmentsigned,
                coalesce (k.investmentsignedamt, nullifzero(i.investmentsignedamt)) as investmentsignedamt,
                dense_rank() over (partition by retrocontractid, configdate order by sp.priority asc) rnk
            from 
                economic_model_raw.retroprogram r
                inner join sourcepriority sp on r.source_db = sp.source_db
                inner join economic_model_raw.spinsurer s on s.retroprogramid = r.retroprogramid and s.source_db = r.source_db and s.isactive
                inner join economic_model_raw.retroinvestor i on i.spinsurerid = s.spinsurerid and i.source_db = r.source_db and i.isactive
                -- inner join economic_model_raw.retrocommission j on j.retrocommissionid = i.retrocommissionid and j.source_db = r.source_db and j.isactive
                inner join economic_model_raw.cedent c on c.cedentid = s.insurerid and c.source_db = r.source_db and c.isactive
                -- break up by owning contract (one invetor might end up split up amongst multiple contract, since a retroprogram can be part of many contracts)
                inner join economic_model_revoext.retroprogramcontractmapping rgm on rgm.retroprogramid = concat(r.source_db, '_', r.retroprogramid)
                left join economic_model_raw.retroinvestorreset k on k.retroinvestorid = i.retroinvestorid and k.source_db = r.source_db and k.isactive = 1
            where 
                r.isactive = 1
        )
        select 
            * exclude rnk 
        from 
            ranked 
        where
            rnk = 1
        order by
            retrocontractid, configdate;

            
    -- the problem: investors that belong to the same contract should be grouped when split up
    -- amongst different retroprograms. There is no ID in revo that lets us link them up, so we
    -- use an algorithm to group them. If investors belong to different retroprograms, but share
    -- the same cedentname, segregatedaccount and their retroprograms are in the same contract,
    -- then we treat them as the same investor. When picking their settings (investmentsigned),
    -- we'll prefer bermuda over morristown in case of conflict, but that's part of the next query.
    create or replace table economic_model_staging.retrocontractinvestor as
        with inv as (
            select
                concat(r.source_db, '_', i.retroinvestorid) retroinvestorid,
                concat(r.source_db, '_', r.retroprogramid) retroprogramid,
                i.name,
                rgm.retrocontractid,
                segregatedaccount,
                trim(c.name) as reinsurername,
                coalesce(k.startdate, r.inception) as configdate,
                j.brokerage,
                i.override as commission,
                i.profitcomm as profitcommission,
                CASE WHEN i.RHOE  > 0 THEN 0 ELSE i.HURDLERATE END AS ReinsuranceExpensesOnCededCapital,
                i.RHOE AS ReinsuranceExpensesOnCededPremium
            from 
                economic_model_raw.retroprogram r
                inner join economic_model_raw.spinsurer s on s.retroprogramid = r.retroprogramid and s.source_db = r.source_db and s.isactive
                inner join economic_model_raw.retroinvestor i on i.spinsurerid = s.spinsurerid and i.source_db = r.source_db and i.isactive
                inner join economic_model_raw.retrocommission j on j.retrocommissionid = i.retrocommissionid and j.source_db = r.source_db and j.isactive
                inner join economic_model_raw.cedent c on c.cedentid = s.insurerid and c.source_db = r.source_db and c.isactive
                inner join economic_model_revoext.retroprogramcontractmapping rgm on rgm.retroprogramid = concat(r.source_db, '_', r.retroprogramid)
                left join economic_model_raw.retroinvestorreset k on k.retroinvestorid = i.retroinvestorid and k.source_db = r.source_db and k.isactive = 1
            where 
                r.isactive = 1
            order by
                retrocontractid, configdate
        )
        , matched as (
            select 
                a.retroinvestorid g, b.* 
            from 
                inv a
                inner join inv b on 
                    -- include self in group
                    (a.retroinvestorid = b.retroinvestorid)
                    -- include investors from different retroprogramid but same segregatedaccount/cedent/retrocontractid
                    OR
                    (
                        a.retroprogramid <> b.retroprogramid
                        and a.retrocontractid = b.retrocontractid
                        and a.segregatedaccount = b.segregatedaccount
                        and a.reinsurername = b.reinsurername
                    )
        )
        select distinct
            -- We might have duplicates, since a group consisting of a&b is the same as a group consisting of b&a. That's why we need:
            -- a) the order-by clause in listagg
            -- b) distinct in the select list
            concat(retrocontractid, '[', listagg(distinct retroinvestorid, '&') within group (order by retroinvestorid), ']') retrocontractinvestorid,
            listagg(distinct retroinvestorid, '&') within group (order by retroinvestorid) retroinvestorIds,
            max_by(name, configdate) Name,
            max_by(brokerage, configdate) brokerage,
            max_by(commission, configdate) commission,
            max_by(profitcommission, configdate) profitcommission,
            max_by(ReinsuranceExpensesOnCededCapital, configdate) ReinsuranceExpensesOnCededCapital,
            max_by(ReinsuranceExpensesOnCededPremium, configdate) ReinsuranceExpensesOnCededPremium,
            retrocontractid,
            segregatedaccount,
            ReinsurerName
        from
            matched
         group by 
            retrocontractid, segregatedaccount, reinsurername, g
        order by 
            retrocontractinvestorid, 
            retrocontractid,
            segregatedaccount,
            Name,
            ReinsurerName;


    create or replace temp table economic_model_staging.idMapping as 
        select distinct
            value as retroInvestorId, rci.retrocontractinvestorid, rci. retrocontractid
        from 
            economic_model_staging.retrocontractinvestor rci
            -- Note: I didn't know how better to do the join since each investor group only gets an id after grouping
            -- and I couldn't figure out an easier way to map back to retro investor ids. This isn't ideal since it will
            -- break if we change the formatting in the retroinvestorIds column.
            , table(split_to_table(rci.retroinvestorIds, '&'));
    
    
    create or replace table economic_model_staging.RetroAllocation as
        select distinct
            concat(ra.source_db, '_', ra.layerid) layerid, 
            m.retrocontractinvestorid,
            ra.CessionGross
        from 
            economic_model_raw.retroallocation ra
            -- this will increase the number of rows because one investor might have become many due to
            -- its retroprogram participating in multiple retro contracts
            inner join economic_model_staging.idMapping m on concat(source_db, '_', ra.retroinvestorid) = m.retroInvestorId
        where 
            ra.isactive = 1 and ra.isdeleted = 0;


    create or replace table economic_model_staging.RetroInvestmentLeg as
        select 
            -- separate id for each investor / contract configuration
            concat(rc.retroconfigurationid, ' {', inv.retrocontractinvestorid, '}') as RetroInvestmentLegId, 
            -- id of the contract investor (needed )
            rc.retroconfigurationid,
            inv.retrocontractinvestorid,
            max(investmentsigned) investmentsigned, 
            max(investmentsignedamt) investmentsignedamt
        from 
            retroinvestormappingdata m
            inner join economic_model_staging.idMapping im on m.retroinvestorid = im.retroinvestorid and im.retrocontractid = m.retrocontractid
            inner join economic_model_staging.retrocontractinvestor inv on im.retrocontractinvestorid = inv.retrocontractinvestorid
            inner join economic_model_staging.retroconfiguration rc on m.retrocontractid = rc.retrocontractid and m.configdate = rc.startdate
        where
            -- note: there are a number of rows (17 at the time of this comment) that fail this criteria. todo: look into why. 
            -- For exmaple, retrocontractinvestorid: ARL_217 [Nautical Underwriting Managers / Nautical], retroconfigurationid:ARL_217[1]
            investmentsigned >0 or investmentsignedamt > 0
        group by 
            inv.retrocontractinvestorid,
            rc.retroconfigurationid;

End
$$;