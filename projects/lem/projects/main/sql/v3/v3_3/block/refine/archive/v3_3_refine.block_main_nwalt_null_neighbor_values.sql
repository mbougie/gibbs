-----Query returned successfully: 813096 rows affected, 2138390 ms execution time.
-----Query returned successfully: 813096 rows affected, 3225107 ms execution time.  <----both columns
-----Query returned successfully: 813096 rows affected, 5264813 ms execution time.


----

create table v3_3_refine.block_main_nwalt_null as 

SELECT 
  * 
FROM 
  v3_3_main.block_main
WHERE nwalt IS NULL

order by hectares desc;



/*
dataset to show all null values for nwalt and nwalt_rc as well

reason for these being null:
1. no raster center inside of polygon (these are very small or very linear (corridor)polygons)
2. larger polygons with no raster under it (i.e. islands)
3. for nwalt_rc some data



things to do:



what have I learned:
1. need to go back and fill small poygons (i.e. refinement)  --- for sure!!
2. need to null out polygons that have been reclassed?  --- TBD



Questions:
Do I need to refine nwalt 21 records or can I just reference the nwalt_rc column?




*/




create table v3_3_refine.v3_3_block_main_nwalt_null_neighbor_values as

SELECT 
  v3_3_block_main_nwalt_null.geoid, 
  ---v3_3_block_main_nwalt_null.neighbor_list, 
  ---v3_3_block_main_nwalt_null.nwalt, 
  ---v3_3_block_main_nwalt_null.nwalt_rc, 
  ---v3_3_block_main_nwalt_null.biomes, 
  ---block_neighbors.src_geoid, 
  ----block_neighbors.nbr_geoid, 
  block_main.nwalt, 
  block_main.nwalt_rc, 
  block_main.biomes
FROM 
  v3_3_qaqc.v3_3_block_main_nwalt_null, 
  v3_core.block_neighbors, 
  v3_3.block_main
WHERE 
  block_neighbors.src_geoid = v3_3_block_main_nwalt_null.geoid AND
  block_main.geoid = block_neighbors.nbr_geoid AND node_count = 0;




create table v3_3_refine.v3_3_block_main_nwalt_null_neighbor_values_mode as 

SELECT
a.geoid as luc_geoid,
a.mode as luc,
b.geoid as biomes_geoid,
b.mode as biomes
FROM
(SELECT 
a.geoid,
mode()
within group(order by block_main.nwalt_rc)
FROM 
  v3_3_refine.v3_3_block_main_nwalt_null_neighbor_values as a, 
  v3_core.block_neighbors, 
  v3_3.block_main
WHERE 
  block_neighbors.src_geoid = a.geoid AND
  block_main.geoid = block_neighbors.nbr_geoid AND node_count = 0
GROUP BY a.geoid
ORDER BY a.geoid) as a
  --count(block_main.nwalt) DESC
--limit 1
INNER JOIN 
(SELECT 
a.geoid,
mode()
within group(order by block_main.biomes)
FROM 
  v3_3_refine.v3_3_block_main_nwalt_null_neighbor_values as a, 
  v3_core.block_neighbors, 
  v3_3.block_main
WHERE 
  block_neighbors.src_geoid = a.geoid AND
  block_main.geoid = block_neighbors.nbr_geoid AND node_count = 0
GROUP BY a.geoid
ORDER BY a.geoid) as b
USING(geoid)