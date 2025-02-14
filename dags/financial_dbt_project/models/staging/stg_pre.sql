{{ config(
    materialized='view'
) }}

SELECT 
    adsh,
    report,
    line,
    stmt,
    tag,
    version,
    plabel
FROM {{ source('financial_data', 'PRE') }}
