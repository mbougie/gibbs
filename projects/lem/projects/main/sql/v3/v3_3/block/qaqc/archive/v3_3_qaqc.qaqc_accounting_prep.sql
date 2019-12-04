DROP TABLE v3_3_qaqc.qaqc_accounting_prep;

CREATE TABLE v3_3_qaqc.qaqc_accounting_prep as 

SELECT 
  0 as index,
  'v3_3.block_main' as dataset,
  ---'main' as stage,
  count(geoid)
FROM 
  v3_3.block_main
----- control stage --------------------
UNION

SELECT 
  1 as index,
  'v3_3_qaqc.v3_3_block_main_nwalt_11_21_60' as dataset,
  --'control' as stage,
  count(geoid)
FROM 
  v3_3_qaqc.v3_3_block_main_nwalt_11_21_60


UNION 

SELECT 
  2 as index,
  'v3_3_qaqc.v3_3_block_main_nwalt_11_21_60_nwalt_rc_has_value' as dataset,
  --'control' as stage,
  count(geoid)
FROM 
  v3_3_qaqc.v3_3_block_main_nwalt_11_21_60_nwalt_rc_has_value


UNION

SELECT 
  3 as index,
  'v3_3_qaqc.v3_3_block_main_nwalt_11_21_60_nwalt_rc_is_null' as dataset,
  --'control' as stage,
  count(geoid)
FROM 
  v3_3_qaqc.v3_3_block_main_nwalt_11_21_60_nwalt_rc_is_null


UNION

SELECT 
  4 as index,
  'v3_3_qaqc.v3_3_block_main_nwalt_null' as dataset,
  --'control' as stage,
  count(geoid)
FROM 
  v3_3_qaqc.v3_3_block_main_nwalt_null

UNION

SELECT 
  5 as index,
  'v3_3_qaqc.v3_3_block_main_nwalt_rc_null' as dataset,
  --'control' as stage,
  count(geoid)
FROM 
  v3_3_qaqc.v3_3_block_main_nwalt_rc_null

UNION

SELECT 
  6 as index,
  'v3_3_qaqc.v3_3_block_main_nwalt_rc_null_diff' as dataset,
  --'control' as stage,
  count(geoid)
FROM 
  v3_3_qaqc.v3_3_block_main_nwalt_rc_null_diff

ORDER BY index
