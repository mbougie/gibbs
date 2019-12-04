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




create table v3_3_refine.block_main_nwalt_null_neighbor_values_t2 as

SELECT 
  a.geoid, 
  ---v3_3_block_main_nwalt_null.neighbor_list, 
  ---v3_3_block_main_nwalt_null.nwalt, 
  ---v3_3_block_main_nwalt_null.nwalt_rc, 
  ---v3_3_block_main_nwalt_null.biomes, 
  ---block_neighbors.src_geoid, 
  ----block_neighbors.nbr_geoid, 
  a.nwalt, 
  a.nwalt_rc, 
  a.biomes
FROM 
  v3_3_refine.block_main_nwalt_null as a, 
  v3_core.block_neighbors as b, 
  v3_3_main.block_main as c
WHERE 
  a.geoid = b.src_geoid AND 
  b.nbr_geoid = c.geoid AND node_count = 0;




create table v3_3_refine.block_main_nwalt_null_neighbor_values_mode as 

SELECT
a.geoid as geoid,
a.mode as luc,
b.mode as biomes
FROM
(SELECT 
a1.geoid,
mode()
within group(order by a1.nwalt_rc)
FROM 
  v3_3_refine.block_main_nwalt_null_neighbor_values as a1, 
  v3_core.block_neighbors a2, 
  v3_3.block_main as a3
WHERE 
  a1.geoid = a2.src_geoid AND
  a2.src_geoid = a3.geoid  AND a2.node_count = 0
GROUP BY a.geoid
ORDER BY a.geoid) as a
  --count(a.nwalt) DESC
--limit 1
INNER JOIN 
(SELECT 
b1.geoid,
mode()
within group(order by b1.biomes)
FROM 
  v3_3_refine.block_main_nwalt_null_neighbor_values as b1, 
  v3_core.block_neighbors b2, 
  v3_3.block_main as b3
WHERE 
  b1.geoid = b2.src_geoid AND
  b2.src_geoid = b3.geoid  AND a2.node_count = 0
GROUP BY b1.geoid
ORDER BY b2.geoid) as b
USING(geoid);



/*
create table v3_3_refine.block_main_nwalt_null_interpolate as 

SELECT 
  v3_3_block_main_nwalt_null_neighbor_values_mode.a_geoid, 
  v3_3_block_main_nwalt_null_neighbor_values_mode.b_geoid, 
  v3_3_block_main_nwalt_null_neighbor_values_mode.luc, 
  v3_3_block_main_nwalt_null_neighbor_values_mode.biomes, 
  a.geom
FROM 
  v3_3_refine.v3_3_block_main_nwalt_null_neighbor_values_mode, 
  v3_3.block_main
WHERE 
  a.geoid = v3_3_block_main_nwalt_null_neighbor_values_mode.a_geoid;
*/

-----Query returned successfully: 813096 rows affected, 738198 ms execution time.
CREATE TABLE v3_3_refine.block_main_nwalt_null_neighbor_values_mode_formatted as 

SELECT 
  a.geoid, 
  a.block_group, 
  a.hectares, 
  a.neighbor_list, 
  b.luc,
  b.biomes,
  a.lng, 
  a.lat, 
  a.geom
FROM v3_3_main.block_main as a INNER JOIN v3_3_refine.block_main_nwalt_null_neighbor_values_mode as b
USING(geoid)