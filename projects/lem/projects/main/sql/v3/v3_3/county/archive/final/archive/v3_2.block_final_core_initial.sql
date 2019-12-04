/*
Description: create table that ONLY contains the records that are not in:
	1 block_final_water
	2 block_final_conservation
	3 block_final_nwalt21
*/
----NOTE this table is getting the majority 

----Query returned successfully: 10196673 rows affected, 1147286 ms execution time.

CREATE TABLE v3_3.block_final_core as 

SELECT 
  block_main.geoid, 
  block_main.block_group, 
  block_main.hectares, 
  block_main.neighbor_list, 
  block_main.nwalt_rc as luc,
  block_main.biomes as biomes,
  block_main.lng, 
  block_main.lat, 
  block_main.geom
FROM 
  v3_3.block_main LEFT OUTER JOIN v3_3.block_replace_nwalt21 USING(geoid) ---- this is a technique to get inverse of join (i.e. records not joined betwen the 2 tables)
WHERE block_replace_nwalt21.geoid IS NULL AND block_main.nwalt NOT IN (11,60)



---------------------------------------------------------------------------------------------------
----ARCHIVED query(remove after qaqc)--------------------------------------------------------------
---------------------------------------------------------------------------------------------------

----Description: create table that ONLY contains the non-null values in the nwalt_rc column and are not equal to 11 in nwalt column in block_main table
----NOTE this table is getting the majority 
----Query returned successfully: 10122499 rows affected, 691541 ms execution time.


/*
CREATE TABLE v3_2.block_core as  

SELECT 
  block_main.geoid, 
  block_main.block_group, 
  block_main.hectares, 
  block_main.neighbor_list, 
  block_main.nwalt_rc as luc,
  block_main.biomes as biomes,
  block_main.lng, 
  block_main.lat, 
  block_main.geom
FROM 
  v3_2.block_main 
---- 
WHERE block_main.nwalt_rc IS NOT NULL AND block_main.nwalt NOT IN (11,60)
*/