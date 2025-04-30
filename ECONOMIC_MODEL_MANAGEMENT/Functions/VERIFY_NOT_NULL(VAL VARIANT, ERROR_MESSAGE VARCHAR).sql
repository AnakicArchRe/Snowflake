CREATE OR REPLACE FUNCTION ECONOMIC_MODEL_MANAGEMENT.VERIFY_NOT_NULL(VAL VARIANT, ERROR_MESSAGE VARCHAR)
RETURNS VARIANT
AS
$$

    -- This function is problematic:
    -- 1. return value is variant, so tables created as a result of a query that uses this function will have the column defined as variant, rather than e.g. float, date, etc... We can deal with this by casting the result to ensure the type gets picked up.
    -- 2. Snowflake was non-deterministically evaluating both branches of the case-when clause, even if it only returns one of them. The fix seems to be to put the expression that throws the exception in the "else" clause. That "seems" to work reliably, though I haven't tested enough times yet. Something to keep an eye out for.
    -- Because of problem nr. 1, I'm not sure if this is a workable solution. I can't make a separate override of this function for each data type because I'd have to support every possible number precision which is completely impractical. As an alternative to casting, I can define the tables explicitly and use inserts instead of create-table-as, but that's even more verbose than casting. 
    -- In the end, I think it's probably best to just use the function only where it's really important to ensure data is not null, and make sure to add the cast.

    CASE
        WHEN val IS not null THEN 
            val
        ELSE 
            to_number('Error: ' || error_message)
    END


$$;