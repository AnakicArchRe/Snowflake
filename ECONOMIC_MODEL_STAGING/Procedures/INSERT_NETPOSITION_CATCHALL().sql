CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.INSERT_NETPOSITION_CATCHALL()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
$$
begin
    
    -- Insert the catch-all retrocontract
    insert into economic_model_revoext.retrocontract (retrocontractid, retroprogramtype, name, level, inception, expiration, exposurestart, exposureend, isspecific, reinsurancebrokerageonnetpremium, commissiononnetpremium, profitcommissionpctofprofit, reinsuranceexpensesoncededcapital, reinsuranceexpensesoncededpremium, isactive, groupname, nonmodeledload, climateload, capitalcalculationlossview, capitalcalculationtargetreturnperiod, pmlload)
    select * from
    values('NET_POSITION', 1, 'ARCH Net Position Catch-All', 10, '2000-1-1', '2100-1-1', '2024-1-1', '2025-1-1', FALSE, 0, 0, 0,0 ,0, TRUE, 'Catch-All'	, 1, 1, 'ARCH', 4, 1 )
    -- ...but only if it doesn't already exist. If it's already in the DB, the user might have adjusted its settings and we don't want to lose those
    where not exists (select * from economic_model_revoext.retrocontract where retrocontractid = 'NET_POSITION');

    -- note: Since investors, retroconfigs and investmentlegs tables are created in during the staging process (as opposed to the retrocontracts table), we don't have to worry about previously existing placeholder data
    
    -- investor for the catch-all contract. 
    insert into economic_model_staging.retrocontractinvestor(retrocontractinvestorid, name, brokerage, commission, profitcommission, reinsuranceexpensesoncededcapital, reinsuranceexpensesoncededpremium, retrocontractid, segregatedaccount, reinsurername)
    values('NET_POSITION_INVESTOR', 'Net position (investor)', 0, 0, 0, 0, 0, 'NET_POSITION', 'ARNet', 'Arch');
    
    -- A single connfiguration, valid from the start
    insert into economic_model_staging.retroconfiguration (retroconfigurationid, retrocontractid, startdate)
    values ('NET_POSITION[0]', 'NET_POSITION', '2000-1-1');
    
    -- A single investmentleg (the contract has just one config/investor)
    insert into economic_model_staging.retroinvestmentleg (retroinvestmentlegid, retroconfigurationid, retrocontractinvestorid, investmentsigned)
    values ('NET_POSITION[0]', 'NET_POSITION[0]', 'NET_POSITION_INVESTOR', 1);
    
    
    -- Tag all portlayers with the catch-all retro
    insert into economic_model_staging.retrotag(retroblockid, periodid, retroconfigurationid, placement)
    select 
        concat(periodid, '->', 'NET_POSITION[0]') as RetroBlockId,
        per.periodid,
        'NET_POSITION[0]',
        1
    from 
        economic_model_staging.portlayerperiod per
        ;

END
$$;