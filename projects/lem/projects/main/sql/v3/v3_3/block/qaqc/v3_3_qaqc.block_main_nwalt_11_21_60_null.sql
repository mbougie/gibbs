-----Query returned successfully: 1,017,011 rows affected, 1183142 ms execution time.

create table v3_3_qaqc.block_main_nwalt_11_21_60_null as 

SELECT 
  * 
FROM 
  v3_3_main.block_main
WHERE nwalt in (11,21,60) OR nwalt IS NULL OR nwalt_rc IS NULL

order by hectares desc;







CREATE TABLE v3_3_qaqc.block_main_nwalt_11_21_60_null_counts as 
------ 
SELECT 
  nwalt,
  nwalt_rc,
  count(geoid),
  'refine ----- replace all these values with the ghhhhh script' as comment
FROM 
  v3_3_qaqc.block_main_nwalt_11_21_60_null
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
  v3_3_qaqc.block_main_nwalt_11_21_60_null
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
  v3_3_qaqc.block_main_nwalt_11_21_60_null
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
  v3_3_qaqc.block_main_nwalt_11_21_60_null
WHERE nwalt = 60
GROUP BY   nwalt,
  nwalt_rc

ORDER BY nwalt 






/*
dataset to show all null values for nwalt and nwalt_rc as well

reason for these being null:
1. no raster center inside of polygon (these are very small or very linear (corridor)polygons)
2. larger polygons with no raster under it (i.e. islands)
3. for nwalt_rc some data



things to do:



what have I learned:
1. need to go back and fill small poygons (i.e. refinement)  --- for sure!!
2. need to null out polygons that have been reclassed?  --- TBD



Questions:
Do I need to refine nwalt 21 records or can I just reference the nwalt_rc column?
Why is 


---*************************************************************************************************************
NOTE: these are all the nwalt records that had values 11,21,60 with ALL nwalt values (null AND integer)
---*****************************************************************************************************


*/


















