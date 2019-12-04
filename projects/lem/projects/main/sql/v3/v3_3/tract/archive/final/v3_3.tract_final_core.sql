/*
Description: create table that ONLY contains the records that are not nwalt raw classes:
	1 water
	2 conservation
	3 major transportation

Reasoning: Removed these classes because they are going to be converted to null or reclassed (i.e. major transportation < 2 hectare or null)
*/




----Query returned successfully: 70820 rows affected, 24030 ms execution time.

CREATE TABLE v3_3.tract_final_core as 

SELECT 
  tract_main.geoid, 
  tract_main.county, 
  tract_main.hectares, 
  tract_main.neighbor_list, 
  tract_main.nwalt_rc as luc,
  tract_main.biomes as biomes,
  tract_main.lng, 
  tract_main.lat, 
  tract_main.geom
FROM 
  v3_3.tract_main 
WHERE tract_main.nwalt NOT IN (11,21,60)



---------------------------------------------------------------------------------------------------
----ARCHIVED query(remove after qaqc)--------------------------------------------------------------
---------------------------------------------------------------------------------------------------

----Description: create table that ONLY contains the non-null values in the nwalt_rc column and are not equal to 11 in nwalt column in tract_main table
----NOTE this table is getting the majority 
----Query returned successfully: 10122499 rows affected, 691541 ms execution time.


/*
CREATE TABLE v3_2.tract_core as  

SELECT 
  tract_main.geoid, 
  tract_main.tract_group, 
  tract_main.hectares, 
  tract_main.neighbor_list, 
  tract_main.nwalt_rc as luc,
  tract_main.biomes as biomes,
  tract_main.lng, 
  tract_main.lat, 
  tract_main.geom
FROM 
  v3_2.tract_main 
---- 
WHERE tract_main.nwalt_rc IS NOT NULL AND tract_main.nwalt NOT IN (11,60)
*/