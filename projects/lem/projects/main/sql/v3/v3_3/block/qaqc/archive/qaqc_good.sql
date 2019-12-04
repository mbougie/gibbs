SELECT 
  nwalt,
  count(geoid) as nwalt_count,
  nwalt_rc,
  count(geoid) as nwalt_rc
FROM 
  v3_3_qaqc.v3_3_block_main_nwalt_11_21_60_null
group by 
  nwalt,
  nwalt_rc
order by nwalt


SELECT 
count(nwalt)
FROM 
v3_3_qaqc.v3_3_block_main_nwalt_11_21_60
where nwalt = 21
