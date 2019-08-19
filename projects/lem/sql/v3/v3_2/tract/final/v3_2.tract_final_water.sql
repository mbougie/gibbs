
-----Description: create table that contains the records with majority 11 (water) and label these records null using the look up table
CREATE TABLE v3_2.tract_final_water as  

SELECT 
  tract_main.geoid,
  tract_main.county,
  tract_main.hectares, 
  tract_main.neighbor_list, 
  --tract_main.nwalt,
  --tract_main.nwalt_rc,
  nwalt_lookup.grouped as luc,
  tract_main.biomes,
  tract_main.lng, 
  tract_main.lat, 
  tract_main.geom
FROM v3_2.tract_main INNER JOIN public.nwalt_lookup
ON tract_main.nwalt = nwalt_lookup.initial  ----need to reference the lookup table to reclassify the raw nwalt values to the grouped values!
 
WHERE tract_main.nwalt = 11
