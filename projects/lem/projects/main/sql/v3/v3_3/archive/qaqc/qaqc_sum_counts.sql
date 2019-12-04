SELECT 
  sum(count) 
FROM 
  v3_3.qaqc_accounting
WHERE dataset in ('v3_3.block_final_core', 'v3_3.qaqc_nwalt21_and_null', 'v3_3.block_final_water', 'v3_3.block_final_conservation')
