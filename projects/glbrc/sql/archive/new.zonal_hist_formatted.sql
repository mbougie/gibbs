create table new.zonal_hist_formatted as 


SELECT 
  zonal_hist.index, 
  zonal_hist.objectid,
  zonal_hist.objectid-1 as value,
  zonal_hist.atlas_stco, 
  zonal_hist.count, 
  zonal_hist.acres
FROM 
  new.zonal_hist

WHERE left(atlas_stco,2) in ('19','27','30','31','38','46','56')
