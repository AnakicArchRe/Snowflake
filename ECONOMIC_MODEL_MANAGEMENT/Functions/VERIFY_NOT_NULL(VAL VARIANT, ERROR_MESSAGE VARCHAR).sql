CREATE OR REPLACE FUNCTION ECONOMIC_MODEL_MANAGEMENT.VERIFY_NOT_NULL(VAL VARIANT, ERROR_MESSAGE VARCHAR)
RETURNS VARIANT
AS
$$

    CASE
        WHEN val IS NULL THEN 
            to_number('Error: ' || error_message)
        ELSE 
            val
    END

$$;