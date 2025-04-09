CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.PREPAREPOWERBIDATA()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin

    -- todo: check if it makes sense to filter by scenario (i.e. only generate ylt blocks for specified scenarios)

    -- calculate YLT blocks for use in PowerBI (subject YLT blocks for product development)
    call economic_model_computed.calculate_subjectblocksylt();

    -- calculate YLT blocks for use in PowerBI (subject YLT blocks for product development)
    call economic_model_computed.calculate_grossblocksylt();

    -- calculate YLT blocks for use in PowerBI (subject YLT blocks for product development)
    call economic_model_computed.calculate_netblocksylt();

end
;