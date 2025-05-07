CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.RESOLVE_SCENARIO_DATA()
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
begin
    
    call economic_model_computed.resolve_scenario_portlayers();
    
    call economic_model_computed.resolve_scenario_retrocontracts();

    call economic_model_computed.resolve_scenario_retroconfigurations();

    call economic_model_computed.resolve_scenario_retroinvestmentlegs();

end
;