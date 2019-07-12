CREATE TABLE intact_lands.intact_lands_per_county as

SELECT
  counties.atlas_name,  
  sum(intactlands_union_pad_raster_rc_cdl30_2015_table.acres) acres,
 (sum(intactlands_union_pad_raster_rc_cdl30_2015_table.acres)/counties.acres_calc)*100 as perc_county,
 counties.geom
  
  
FROM 
  intact_lands.intactlands_union_pad_raster_rc_cdl30_2015_table

INNER JOIN
spatial.counties
ON intactlands_union_pad_raster_rc_cdl30_2015_table.atlas_stco = counties.atlas_stco

GROUP by 
  counties.atlas_name,
  counties.acres_calc,
  counties.geom
