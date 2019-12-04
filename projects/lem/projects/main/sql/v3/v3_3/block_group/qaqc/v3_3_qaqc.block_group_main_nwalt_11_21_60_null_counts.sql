CREATE TABLE v3_3_qaqc.block_group_main_nwalt_11_21_60_null_counts as 
------ 
SELECT 
  nwalt,
  nwalt_rc,
  count(geoid),
  'refine ----- replace all these values with the ghhhhh script' as comment
FROM 
  v3_3_qaqc.block_group_main_nwalt_11_21_60_null
WHERE nwalt IS NULL
GROUP BY
  nwalt,
  nwalt_rc


UNION

SELECT 
    nwalt,
  nwalt_rc,
  count(geoid),
  'refine ----- null all these records for water using the blsah script' as comment
FROM 
  v3_3_qaqc.block_group_main_nwalt_11_21_60_null
WHERE nwalt = 11
GROUP BY
  nwalt,
  nwalt_rc


UNION

SELECT 
   nwalt,
  nwalt_rc,
  count(geoid),
  'do NOT refine ---- use the nwalt_rc column.  If null in both nwalt and nwalt_rc it is OK to leave null  <---- these records can be part of the refine_core dataset' as comment
FROM 
  v3_3_qaqc.block_group_main_nwalt_11_21_60_null
WHERE nwalt = 21
GROUP BY   nwalt,
  nwalt_rc


UNION

SELECT 
    nwalt,
  nwalt_rc,
  count(geoid),
  'refine ---- null all these records for conservation using the blsah script' as comment
FROM 
  v3_3_qaqc.block_group_main_nwalt_11_21_60_null
WHERE nwalt = 60
GROUP BY   nwalt,
  nwalt_rc

ORDER BY nwalt 

