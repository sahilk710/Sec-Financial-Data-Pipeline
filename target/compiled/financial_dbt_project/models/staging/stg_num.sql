

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
FROM ASSIGNMENT2_TEAM1.FACT_TABLE_STAGING.NUM