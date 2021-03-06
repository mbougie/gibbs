﻿----Query returned successfully: 8 rows affected, 790443 ms execution time.
----Query returned successfully: 8 rows affected, 713460 ms execution time.
----Query returned successfully: 9 rows affected, 755459 ms execution time.
----Query returned successfully: 10 rows affected, 1098757 ms execution time.
DROP TABLE v3_3.qaqc_accounting_block_group;

CREATE TABLE v3_3.qaqc_accounting_block_group as 


----- control stage --------------------
SELECT 
  0 as index,
  'v3_core.block_group' as dataset,
  'control' as stage,
  count(geoid)
FROM 
  v3_core.block_group


UNION

----- main stage --------------------
SELECT 
  1 as index,
  'v3_3.block_group_main' as dataset,
  'main' as stage,
  count(geoid)
FROM 
  v3_3.block_group_main

UNION 

SELECT
  2 as index,
  'v3_3.block_group_nbrlist' as dataset,
  'main' as stage,
  count(geoid)
FROM 
  v3_3.block_group_nbrlist


UNION

----- refine stage --------------------
SELECT 
  3 as index,
  'v3_3.block_group_refine_nwalt21_and_null' as dataset,
  'refine' as stage, 
  count(geoid)
FROM 
  v3_3.block_group_refine_nwalt21_and_null

UNION


----- final stage --------------------

SELECT 
  4 as index,
  'v3_3.block_group_final_core' as dataset, 
  'final' as stage,
  count(geoid)
FROM 
  v3_3.block_group_final_core

UNION

SELECT 
  5 as index,
  'v3_3.block_group_final_nwalt21_refined' as dataset, 
  'final' as stage,
  count(geoid)
FROM 
  v3_3.block_group_final_nwalt21_refined


UNION

SELECT 
  5 as index,
  'v3_3.block_group_final_nwalt21_unrefined' as dataset, 
  'final' as stage,
  count(geoid)
FROM 
  v3_3.block_group_final_nwalt21_unrefined



UNION

SELECT 
  6 as index,
  'v3_3.block_group_final_water' as dataset,
  'final' as stage, 
  count(geoid)
FROM 
  v3_3.block_group_final_water


UNION

SELECT
  7 as index, 
  'v3_3.block_group_final_conservation' as dataset,
  'final' as stage, 
  count(geoid)
FROM 
  v3_3.block_group_final_conservation

UNION

SELECT 
  8 as index,
  'v3_3.block_group_final_union' as dataset,
  'final' as stage, 
  count(geoid)
FROM 
  v3_3.block_group_final_union




--------qaqc -------------------
/*
UNION

SELECT 
  9 as index,
  'v3_3.qaqc_nwalt21_and_null' as dataset,
  'qaqc' as stage, 
  count(geoid)
FROM 
  v3_3.qaqc_nwalt21_and_null
*/


ORDER BY index
