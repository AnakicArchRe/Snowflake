CREATE OR REPLACE FUNCTION ECONOMIC_MODEL_COMPUTED.CONCAT_NON_NULL(S1 VARCHAR, S2 VARCHAR, S3 VARCHAR, S4 VARCHAR, S5 VARCHAR, S6 VARCHAR, S7 VARCHAR, S8 VARCHAR, S9 VARCHAR, S10 VARCHAR)
RETURNS VARCHAR(16777216)
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