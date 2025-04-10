CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_STAGING.populate_staging_data()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
BEGIN

    call economic_model_staging.populate_Submissions();

    call economic_model_staging.populate_Programs();
    
    call economic_model_staging.populate_Portfolios();

    call economic_model_staging.populate_PortLayers();
        
    call economic_model_staging.populate_RetroConfigurations();

    call economic_model_staging.populate_RetroPrograms();

    call economic_model_staging.populate_RetroContracts();

    call economic_model_staging.populate_fx();
    
    // todo: we're using reference portfolios to determine which portlayer cedes to which
    // retroprogram. We should use the new http endpoint for that instead, and get rid of
    // the referenceportfolios table. We're switching to per-scenario referenceportfolios
    // anyway.
    
    // This should be a "private" table, i.e. only for use in this schema.
    // It's needed for preparing periods (start/end ceding affects periods)
    // as well as retrotags (needs to know which retros to apply to each period)
    call economic_model_staging.Populate_PortLayerCessions();

    // This creates tables related to retro investors (retroinvestor, retroinvestmentleg)
    call economic_model_staging.populate_retroinvestor_data();
    
    /*
    From this point we start slicing up portlayers into stable periods. If, in the scenario editor, we want to support
    making changes that affect stable periods (e.g. changing retro inception/expiration, or retro type), we'll have to move
    the below tables after the scenario data is available (in the _computed schema) and make them scenario dependent, either
    by splitting up rows for each scenario, or by using a diff approach (adding compensatory periods that we add to the 
    base scenario).
    */

    call economic_model_staging.populate_portlayerperiods();
    
    call economic_model_staging.Populate_RetroTags();

    call economic_model_staging.Populate_Yelpt();

    call economic_model_staging.Insert_NetPosition_CatchAll();
    
END
;