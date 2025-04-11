CREATE OR REPLACE FUNCTION economic_model_computed.FORMAT_PERCENT(n NUMBER, max_decimals INT)
RETURNS STRING
LANGUAGE SQL
AS
$$
  format_number(n*100, max_decimals) || '%'
$$;