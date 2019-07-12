SELECT
  states.atlas_name,  
  sum(intactlands_union_pad_raster_rc_cdl30_2015_table.acres) acres,
  (sum(intactlands_union_pad_raster_rc_cdl30_2015_table.acres))/states.acres_calc as perc_state
  
  
FROM 
  intact_lands.intactlands_union_pad_raster_rc_cdl30_2015_table

INNER JOIN
spatial.states 
ON LEFT(intactlands_union_pad_raster_rc_cdl30_2015_table.atlas_stco,2) = states.atlas_st

GROUP by states.atlas_name,states.acres_calc