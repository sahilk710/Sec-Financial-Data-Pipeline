SELECT n.adsh 
FROM {{ ref('stg_num') }} n
LEFT JOIN {{ ref('stg_sub') }} s
ON n.adsh = s.adsh
WHERE s.adsh IS NULL
