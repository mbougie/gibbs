----Query returned successfully: 8 rows affected, 790443 ms execution time.
----Query returned successfully: 8 rows affected, 713460 ms execution time.
----Query returned successfully: 9 rows affected, 755459 ms execution time.
DROP TABLE v3_3.qaqc_accounting;

create table v3_3.qaqc_accounting as 


----- control stage --------------------
SELECT 
  0 as index,
  'v3_core.block' as dataset,
  'control' as stage,
  count(geoid)
FROM 
  v3_core.block


UNION

----- main stage --------------------
SELECT 
  1 as index,
  'v3_3.block_main' as dataset,
  'main' as stage,
  count(geoid)
FROM 
  v3_3.block_main

UNION 

SELECT
  2 as index,
  'v3_3.block_nbrlist' as dataset,
  'main' as stage,
  count(geoid)
FROM 
  v3_3.block_nbrlist


UNION

----- refine stage --------------------
SELECT 
  3 as index,
  'v3_3.block_refine_nwalt21' as dataset,
  'refine' as stage, 
  count(geoid)
FROM 
  v3_3.block_refine_nwalt21

UNION


----- final stage --------------------

SELECT 
  4 as index,
  'v3_3.block_final_core' as dataset, 
  'final' as stage,
  count(geoid)
FROM 
  v3_3.block_final_core

UNION

SELECT 
  5 as index,
  'v3_3.block_final_nwalt21' as dataset, 
  'final' as stage,
  count(geoid)
FROM 
  v3_3.block_final_nwalt21


UNION

SELECT 
  6 as index,
  'v3_3.block_final_water' as dataset,
  'final' as stage, 
  count(geoid)
FROM 
  v3_3.block_final_water


UNION

SELECT
  7 as index, 
  'v3_3.block_final_conservation' as dataset,
  'final' as stage, 
  count(geoid)
FROM 
  v3_3.block_final_conservation

UNION

SELECT 
  8 as index,
  'v3_3.block_final_union' as dataset,
  'final' as stage, 
  count(geoid)
FROM 
  v3_3.block_final_union


UNION

--------qaqc -------------------

SELECT 
  9 as index,
  'v3_3.qaqc_nwalt21_and_null' as dataset,
  'qaqc' as stage, 
  count(geoid)
FROM 
  v3_3.qaqc_nwalt21_and_null


ORDER BY index

