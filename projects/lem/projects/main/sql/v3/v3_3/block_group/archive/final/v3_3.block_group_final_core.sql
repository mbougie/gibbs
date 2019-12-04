/*
Description: create table that ONLY contains the records that are not nwalt raw classes:
	1 water
	2 conservation
	3 major transportation

Reasoning: Removed these classes because they are going to be converted to null or reclassed (i.e. major transportation < 2 hectare or null)
*/




----Query returned successfully: 9177882 rows affected, 919617 ms execution time.

CREATE TABLE v3_3.block_group_final_core as 
SELECT 
  block_group_main.geoid, 
  block_group_main.tract, 
  block_group_main.hectares, 
  block_group_main.neighbor_list, 
  block_group_main.nwalt_rc as luc,
  block_group_main.biomes as biomes,
  block_group_main.lng, 
  block_group_main.lat, 
  block_group_main.geom
FROM 
  v3_3.block_group_main 
WHERE block_group_main.nwalt NOT IN (11,21,60)



---------------------------------------------------------------------------------------------------
----ARCHIVED query(remove after qaqc)--------------------------------------------------------------
---------------------------------------------------------------------------------------------------

----Description: create table that ONLY contains the non-null values in the nwalt_rc column and are not equal to 11 in nwalt column in block_group_main table
----NOTE this table is getting the majority 
----Query returned successfully: 10122499 rows affected, 691541 ms execution time.


/*
CREATE TABLE v3_2.block_group_core as  

SELECT 
  block_group_main.geoid, 
  block_group_main.block_group_group, 
  block_group_main.hectares, 
  block_group_main.neighbor_list, 
  block_group_main.nwalt_rc as luc,
  block_group_main.biomes as biomes,
  block_group_main.lng, 
  block_group_main.lat, 
  block_group_main.geom
FROM 
  v3_2.block_group_main 
---- 
WHERE block_group_main.nwalt_rc IS NOT NULL AND block_group_main.nwalt NOT IN (11,60)
*/