
-----Description: create table that contains the records with majority 11 (water) and label these records null using the look up table
CREATE TABLE v3_2.county_final_water as  

SELECT 
  county_main.geoid, 
  county_main.hectares, 
  county_main.neighbor_list, 
  --county_main.nwalt,
  --county_main.nwalt_rc,
  nwalt_lookup.grouped as luc,
  county_main.biomes,
  county_main.lng, 
  county_main.lat, 
  county_main.geom
FROM v3_2.county_main INNER JOIN public.nwalt_lookup
ON county_main.nwalt = nwalt_lookup.initial  ----need to reference the lookup table to reclassify the raw nwalt values to the grouped values!
 
WHERE county_main.nwalt = 11
