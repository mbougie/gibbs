
create table v3_3_refine.block_group_main_nwalt_null_neighbor_values as

SELECT 
  a.geoid, 
  b.nbr_geoid,
  c.nwalt as nbr_nwalt,
  c.nwalt_rc as nbr_nwalt_rc, 
  c.biomes as nbr_biomes,
  a.geom
FROM 
  v3_3_main.block_group_main as a INNER JOIN
  v3_core.block_group_neighbors as b 
ON 
  a.geoid = b.src_geoid
INNER JOIN v3_3_main.block_group_main as c
ON 
  c.geoid = b.nbr_geoid
WHERE node_count = 0
AND a.nwalt = 21 AND a.nwalt_rc IS NULL


ORDER BY geoid;




CREATE TABLE v3_3_refine.block_group_main_nwalt_null_neighbor_values_mode as 

SELECT
a.geoid as geoid,
a.mode as luc,
b.mode as biomes
FROM
(SELECT 
a1.geoid,
mode()
within group(order by a1.nbr_nwalt_rc)
FROM 
  v3_3_refine.block_group_main_nwalt_null_neighbor_values as a1, 
  v3_core.block_group_neighbors a2
WHERE 
  a1.geoid = a2.src_geoid AND a2.node_count = 0
GROUP BY a1.geoid
ORDER BY a1.geoid) as a
  --count(a.nwalt) DESC
--limit 1
INNER JOIN 
(SELECT 
b1.geoid,
mode()
within group(order by b1.nbr_biomes)
FROM 
  v3_3_refine.block_group_main_nwalt_null_neighbor_values as b1, 
  v3_core.block_group_neighbors b2
WHERE 
  b1.geoid = b2.src_geoid AND b2.node_count = 0
GROUP BY b1.geoid
ORDER BY b1.geoid) as b
USING(geoid);



/*
CREATE TABLE v3_3_refine.block_group_main_nwalt_null_neighbor_values_mode as 

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
  v3_3_refine.block_group_main_nwalt_null_neighbor_values as a1, 
  v3_core.block_group_neighbors a2, 
  v3_3_main.block_group_main as a3
WHERE 
  a1.geoid = a2.src_geoid AND
  a2.src_geoid = a3.geoid  AND a2.node_count = 0
GROUP BY a1.geoid
ORDER BY a1.geoid) as a
  --count(a.nwalt) DESC
--limit 1
INNER JOIN 
(SELECT 
b1.geoid,
mode()
within group(order by b1.biomes)
FROM 
  v3_3_refine.block_group_main_nwalt_null_neighbor_values as b1, 
  v3_core.block_group_neighbors b2, 
  v3_3_main.block_group_main as b3
WHERE 
  b1.geoid = b2.src_geoid AND
  b2.src_geoid = b3.geoid  AND b2.node_count = 0
GROUP BY b1.geoid
ORDER BY b1.geoid) as b
USING(geoid);
*/





-----Query returned successfully: 813096 rows affected, 738198 ms execution time.
CREATE TABLE v3_3_refine.block_group_main_nwalt_null_neighbor_values_mode_formatted as 

SELECT 
  a.geoid, 
  a.tract, 
  a.hectares, 
  a.neighbor_list, 
  b.luc,
  b.biomes,
  a.lng, 
  a.lat, 
  a.geom
FROM v3_3_main.block_group_main as a INNER JOIN v3_3_refine.block_group_main_nwalt_null_neighbor_values_mode as b
USING(geoid)








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

