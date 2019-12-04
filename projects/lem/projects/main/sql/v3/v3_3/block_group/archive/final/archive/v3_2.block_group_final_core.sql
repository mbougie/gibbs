----Description: create table that ONLY contains the non-null values in the nwalt_rc column and are not equal to 11 in nwalt column in block_group_main table
----NOTE this table is getting the majority 
----Query returned successfully: 213943 rows affected, 113880 ms execution time.
CREATE TABLE v3_2.block_group_core as  

SELECT 
  block_group_main.geoid, 
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
WHERE block_group_main.nwalt_rc IS NOT NULL AND block_group_main.nwalt NOT IN (11)
