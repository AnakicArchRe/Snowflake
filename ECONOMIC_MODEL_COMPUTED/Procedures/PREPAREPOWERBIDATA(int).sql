CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.PREPAREPOWERBIDATA(scenarioId int)
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    -- calculate YLT blocks for use in PowerBI (subject YLT blocks for product development)
    call economic_model_computed.calculate_subjectblocksylt(:scenarioId);

    -- calculate YLT blocks for use in PowerBI (subject YLT blocks for product development)
    call economic_model_computed.calculate_grossblocksylt(:scenarioId);

    -- calculate YLT blocks for use in PowerBI (subject YLT blocks for product development)
    call economic_model_computed.calculate_netblocksylt(:scenarioId);

end;