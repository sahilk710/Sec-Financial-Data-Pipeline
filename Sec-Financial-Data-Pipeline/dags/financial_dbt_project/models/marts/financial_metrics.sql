{{ config(
    materialized='table'
) }}

WITH balance_sheet AS (
    SELECT *
    FROM {{ ref('fact_balance_sheet') }}
),

income_stmt AS (
    SELECT *
    FROM {{ ref('fact_income_statement') }}
),

cash_flow AS (
    SELECT *
    FROM {{ ref('fact_cashflow') }}
)

SELECT 
    b.adsh,
    b.tag as balance_sheet_tag,
    b.value as balance_sheet_value,
    i.tag as income_stmt_tag,
    i.value as income_stmt_value,
    c.tag as cashflow_tag,
    c.value as cashflow_value
FROM balance_sheet b
LEFT JOIN income_stmt i ON b.adsh = i.adsh
LEFT JOIN cash_flow c ON b.adsh = c.adsh
