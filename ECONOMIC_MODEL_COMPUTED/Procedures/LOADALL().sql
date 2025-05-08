CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.LOADALL()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    -- load scenario-independent data
    call economic_model_computed.stagemodel();

    -- resolve scenario data, calculate blocks and contract/investor results
    call economic_model_computed.process_model(null);

    -- precalculate ylt for blocks, for better powerbi performance
    call economic_model_computed.PreparePowerBIData(null);

end
;