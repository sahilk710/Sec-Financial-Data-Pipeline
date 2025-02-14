{{ config(
    materialized='view'
) }}

SELECT
    adsh,
    tag,
    version,
    ddate,
    qtrs,
    uom,
    segments,
    coreg,
    COALESCE(value, -1) AS value,
    COALESCE(footnote, 'nan') AS footnote
FROM {{ source('financial_data', 'NUM') }}