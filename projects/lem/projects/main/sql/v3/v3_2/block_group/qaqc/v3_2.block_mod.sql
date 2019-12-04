----Query returned successfully: 7655534 rows affected, 11849589 ms execution time.


CREATE TABLE v3_2.block_mod as 

SELECT 
  block.objectid, 
  block.geoid, 
  block_group.geoid as block_group, 
  block.shape_length, 
  block.shape_area, 
  block.wkb_geometry
FROM 
  v3_2.block, 
  v3_2.block_group


WHERE block.wkb_geometry && block_group.wkb_geometry 
AND ST_WITHIN(block.wkb_geometry, block_group.wkb_geometry)
