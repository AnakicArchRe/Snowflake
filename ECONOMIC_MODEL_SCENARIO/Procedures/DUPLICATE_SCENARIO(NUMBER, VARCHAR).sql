CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_SCENARIO.DUPLICATE_SCENARIO(ORIGINAL_SCENARIO_ID NUMBER, NEW_SCENARIO_NAME VARCHAR)
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
DECLARE new_scenario_id NUMBER;
begin
    
    let uuidstr := (SELECT UUID_STRING());

    INSERT INTO economic_model_scenario.scenario(name,description, isactive, sortorder, parentscenarioid, analysis_portfolioids, analysis_retrocontractids, fxdate, netcessionslockin)
    select 
        :uuidstr || coalesce(:new_scenario_name, name || '-duplicate') as name, description, isactive, sortorder, parentscenarioid, analysis_portfolioids, analysis_retrocontractids, fxdate, netcessionslockin
    from 
        economic_model_scenario.scenario where scenarioid = :original_scenario_id;
    
    // find the newly id of the newly inserted row
    set new_scenario_id := (select scenarioid from economic_model_scenario.scenario where name like :uuidstr || '%' );

    update economic_model_scenario.scenario
    set name = substr(name, 37)
    where name like :uuidstr || '%';

    insert into economic_model_scenario.portlayer_override(scenarioid, portlayerid, sharefactor, premiumfactor)
    select :new_scenario_id, portlayerid, sharefactor, premiumfactor from economic_model_scenario.portlayer_override
    where scenarioid = :original_scenario_id;

    insert into economic_model_scenario.retroconfiguration_override(scenarioid, retroconfigurationid, targetcollateraloverride)
    select :new_scenario_id, retroconfigurationid, targetcollateraloverride 
    from economic_model_scenario.retroconfiguration_override
    where scenarioid = :original_scenario_id;

    insert into economic_model_scenario.RETROCONTRACT_OVERRIDE(scenarioid, RETROCONTRACTID, LEVEL, ISACTIVE, commission, profitcommission, reinsurancebrokerageonnetpremium, reinsuranceexpensesoncededcapital, reinsuranceexpensesoncededpremium, targetcollateralcalculated)
    select :new_scenario_id, RETROCONTRACTID, LEVEL, ISACTIVE, commission, profitcommission, reinsurancebrokerageonnetpremium, reinsuranceexpensesoncededcapital, reinsuranceexpensesoncededpremium, targetcollateralcalculated 
    from economic_model_scenario.RETROCONTRACT_OVERRIDE
    where scenarioid = :original_scenario_id;

    insert into economic_model_scenario.RETROINVESTMENTLEG_OVERRIDE(scenarioid, RETROINVESTMENTLEGID, INVESTMENTSIGNEDPCTENTRY, INVESTMENTSIGNEDPCT, INVESTMENTSIGNEDAMT, INVESTMENTCALCULATEDPCT)
    select :new_scenario_id, RETROINVESTMENTLEGID, INVESTMENTSIGNEDPCTENTRY, INVESTMENTSIGNEDPCT, INVESTMENTSIGNEDAMT, INVESTMENTCALCULATEDPCT 
    from economic_model_scenario.RETROINVESTMENTLEG_OVERRIDE
    where scenarioid = :original_scenario_id;

    insert into economic_model_scenario.TOPUPZONE_OVERRIDE(scenarioid, TOPUPZONEID, LINTEAU_SHAREFACTOR, TOPUP_SHAREFACTOR, MIDDLE_SHAREFACTOR, LINTEAU_PREMIUMFACTOR, TOPUP_PREMIUMFACTOR, MIDDLE_PREMIUMFACTOR)
    select :new_scenario_id, TOPUPZONEID, LINTEAU_SHAREFACTOR, TOPUP_SHAREFACTOR, MIDDLE_SHAREFACTOR, LINTEAU_PREMIUMFACTOR, TOPUP_PREMIUMFACTOR, MIDDLE_PREMIUMFACTOR
    from economic_model_scenario.TOPUPZONE_OVERRIDE
    where scenarioid = :original_scenario_id;
    
    return new_scenario_id;

END
;