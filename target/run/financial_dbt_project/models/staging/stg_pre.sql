
  create or replace   view ASSIGNMENT2_TEAM1.FACT_TABLE_STAGING_FACT_TABLE_STAGING.stg_pre
  
   as (
    

SELECT 
    adsh,
    report,
    line,
    stmt,
    tag,
    version,
    plabel
FROM ASSIGNMENT2_TEAM1.FACT_TABLE_STAGING.PRE
  );

