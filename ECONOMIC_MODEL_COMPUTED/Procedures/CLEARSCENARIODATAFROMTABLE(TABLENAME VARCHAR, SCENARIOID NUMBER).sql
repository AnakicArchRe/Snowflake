CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_COMPUTED.CLEARSCENARIODATAFROMTABLE(TABLENAME VARCHAR, SCENARIOID NUMBER)
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
declare x varchar;
begin

    set x := concat('economic_model_computed.', :tablename);

    delete from identifier(:x)
    where
        -- clear old data for the scenarios we're about to calculate
        scenarioid in (select scenarioid from economic_model_scenario.scenario where scenarioid = :scenarioid or :scenarioid is null)
        -- clear any orphaned data (scenarios deleted or no longer active)
        or scenarioid not in (select scenarioid from economic_model_scenario.scenario where isactive = 1);
    
    
end
;