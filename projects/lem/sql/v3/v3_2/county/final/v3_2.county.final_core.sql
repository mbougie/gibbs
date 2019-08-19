----Description: create table that ONLY contains the non-null values in the nwalt_rc column and are not equal to 11 in nwalt column in county_main table
----NOTE this table is getting the majority 

----Query returned successfully: 3106 rows affected, 18343 ms execution time.
CREATE TABLE v3_2.county_final_core as  

SELECT 
  county_main.geoid, 
  county_main.hectares, 
  county_main.neighbor_list, 
  county_main.nwalt_rc as luc,
  county_main.biomes as biomes,
  county_main.lng, 
  county_main.lat, 
  county_main.geom
FROM 
  v3_2.county_main 
---- 
WHERE county_main.nwalt_rc IS NOT NULL AND county_main.nwalt NOT IN (11)
