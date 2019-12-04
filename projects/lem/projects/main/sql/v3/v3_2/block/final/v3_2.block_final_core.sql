----Description: create table that ONLY contains the non-null values in the nwalt_rc column and are not equal to 11 in nwalt column in block_main table
----NOTE this table is getting the majority 
----Query returned successfully: 10122499 rows affected, 691541 ms execution time.
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
WHERE block_main.nwalt_rc IS NOT NULL AND block_main.nwalt NOT IN (11)
