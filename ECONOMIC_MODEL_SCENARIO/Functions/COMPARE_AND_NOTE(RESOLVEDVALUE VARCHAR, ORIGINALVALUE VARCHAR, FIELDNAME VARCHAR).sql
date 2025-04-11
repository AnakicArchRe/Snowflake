CREATE OR REPLACE FUNCTION ECONOMIC_MODEL_SCENARIO.COMPARE_AND_NOTE(RESOLVEDVALUE variant, ORIGINALVALUE variant, FIELDNAME VARCHAR)
RETURNS VARCHAR(16777216)
AS
$$

    CASE 
        WHEN resolvedValue is distinct from originalValue THEN concat(fieldName, ' (', originalValue, ' => ', resolvedValue, ')')
        ELSE NULL
    END

$$;

