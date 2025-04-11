CREATE OR REPLACE FUNCTION economic_model_computed.concat_non_null(
    -- delimiter string,// commenting out delimiter, I want to control this centrally, all places should use the same delimiter
    s1 STRING, s2 STRING default null, s3 STRING default null, s4 STRING default null, s5 STRING default null,
    s6 STRING default null, s7 STRING default null, s8 STRING default null, s9 STRING default null, s10 STRING default null
)
RETURNS STRING
LANGUAGE SQL
AS
$$
  ARRAY_TO_STRING(
    ARRAY_CONSTRUCT_COMPACT(
      case when s1 = '' then null else s1 end, 
      case when s2 = '' then null else s2 end, 
      case when s3 = '' then null else s3 end, 
      case when s4 = '' then null else s4 end, 
      case when s5 = '' then null else s5 end, 
      case when s6 = '' then null else s6 end, 
      case when s7 = '' then null else s7 end, 
      case when s8 = '' then null else s8 end, 
      case when s9 = '' then null else s9 end, 
      case when s10 = '' then null else s10 end),
    ', '
  )
$$;

