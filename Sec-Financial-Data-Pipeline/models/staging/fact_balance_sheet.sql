{{ config(
    materialized='table'
) }}

SELECT
    n.adsh,
    n.tag,
    n.value,
    p.stmt,
    p.plabel
FROM {{ ref('stg_num') }} n
JOIN {{ ref('stg_pre') }} p
ON n.adsh = p.adsh AND n.tag = p.tag
WHERE p.stmt = 'BS'
