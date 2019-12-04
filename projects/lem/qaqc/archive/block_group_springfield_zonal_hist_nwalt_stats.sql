SELECT 
  label,
 objectid,
 count,
 total,
 ROUND(((count::numeric/total::numeric)*100),2) as perc
FROM 
  qaqc.block_group_springfield_zonal_hist_nwalt_unique
inner join
 (SELECT 
  sum(count) as total,
  objectid
FROM 
  qaqc.block_group_springfield_zonal_hist_nwalt_unique
  group by objectid
  order by objectid) as denminator
USING(objectid)
order by objectid::numeric, ROUND(((count::numeric/total::numeric)*100),2) DESC



