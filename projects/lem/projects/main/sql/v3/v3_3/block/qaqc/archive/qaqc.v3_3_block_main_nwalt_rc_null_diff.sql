----Query returned successfully: 445994 rows affected, 11172 ms execution time.

CREATE TABLE qaqc.v3_3_block_main_nwalt_rc_null_diff as 
SELECT 
  b.geoid, 
  b.block_group, 
  b.hectares, 
  b.neighbor_list, 
  b.nwalt, 
  b.nwalt_rc, 
  b.biomes, 
  b.lng, 
  b.lat, 
  b.geom
FROM 
  qaqc.v3_3_block_main_nwalt_null as a FULL OUTER JOIN
  qaqc.v3_3_block_main_nwalt_rc_null as b
USING (geoid)
WHERE a.geoid is null
order by b.geoid desc


----note: the reason for the difference is due to no other raster centers being inside of the polygon once the nwalt value (11,21,60) is removed

----Note: these are the null polygons (11,21,60) that where NOT filled with values in nwalt_rc!!