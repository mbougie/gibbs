----Description: create table that ONLY contains the non-null values in the nwalt_rc column and are not equal to 11 in nwalt column in tract_main table
----NOTE this table is getting the majority 

---Query returned successfully: 71644 rows affected, 77811 ms execution time.
CREATE TABLE v3_2.tract_final_core as  

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
  v3_2.tract_main 
---- 
WHERE tract_main.nwalt_rc IS NOT NULL AND tract_main.nwalt NOT IN (11)
