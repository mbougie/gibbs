SELECT 
  * 
FROM 
  v2_3.block_zonal_maj_nwalt, 
  v2_3.block_zonal_maj_rc_nwalt, 
  v2_3.block
WHERE 
  block_zonal_maj_nwalt.geoid = block.geoid AND
  block.geoid = block_zonal_maj_rc_nwalt.geoid AND


block_zonal_maj_rc_nwalt.majority IS NULL
