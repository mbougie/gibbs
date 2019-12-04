create table qaqc.v3_3_block_main_nwalt_11_21_60_nwalt_rc_is_null as

SELECT 
  * 
FROM 
  qaqc.v3_3_block_main_nwalt_11_21_60
WHERE nwalt_rc IS NULL
