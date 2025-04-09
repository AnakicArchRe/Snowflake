CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.STAGEMODEL()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    -- clear old data just in case (we don't want to have any artifacts that belong to a previous run)
    call economic_model_computed.ClearAll();

    -- load raw data
    call economic_model_raw.load_raw_data();

    -- prepare scenario-independent data (portlayercessions, portlayerperiods, yelpt, etc.)
    call economic_model_staging.calculate_model();

end
;