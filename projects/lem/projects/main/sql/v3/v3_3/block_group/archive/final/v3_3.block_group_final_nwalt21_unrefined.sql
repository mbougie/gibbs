----the goal is 229,938!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

-----Description: create table that contains the records with majority 11 (water) and label these records null using the look up table
CREATE TABLE v3_3.block_group_final_nwalt21_unrefined as  

SELECT 
  block_group_main.geoid, 
  block_group_main.tract, 
  block_group_main.hectares, 
  block_group_main.neighbor_list, 
  --block_group_main.nwalt,
  --block_group_main.nwalt_rc,
  nwalt_lookup.reclass_v2 as luc,
  block_group_main.biomes,
  block_group_main.lng, 
  block_group_main.lat, 
  block_group_main.geom
FROM v3_3.block_group_main INNER JOIN public.nwalt_lookup
ON block_group_main.nwalt = nwalt_lookup.initial  ----need to reference the lookup table to reclassify the raw nwalt values to the grouped values!
 
WHERE 
(nwalt IN (21))   --- want records that are nwalt value 21

AND hectares >= 2 ---- want to threshold and only select smaller polygons to fill because large polygons are airports and other large areas


UNION

----have to use union becasue of the way the query is set up and is reference a value in the lookup table to get null (sub-optimal)
SELECT 
  block_group_main.geoid, 
  block_group_main.tract, 
  block_group_main.hectares, 
  block_group_main.neighbor_list, 
  --block_group_main.nwalt,
  --block_group_main.nwalt_rc,
  --nwalt_lookup.reclass_v2 as luc,
  NULL as luc,
  block_group_main.biomes,
  block_group_main.lng, 
  block_group_main.lat, 
  block_group_main.geom
FROM v3_3.block_group_main 
WHERE 
(nwalt IS NULL)   --- NULL

AND hectares >= 2; ---- want to threshold and only select smaller polygons to fill because large polygons are airports and other large areas







-----NOTE!!!!!!!!!!!!!!!!!!!!!!!!!
---- these are the poygons like airports that I do not want to refine BUT STILL want to reclassify as null!!!!!!!!!!!!!!!!!!

----LOGIC: its ok to have null records in this non-refined dataset because the datset should have be filled ??
