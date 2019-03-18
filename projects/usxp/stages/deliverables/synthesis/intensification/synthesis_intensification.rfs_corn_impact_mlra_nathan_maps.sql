create table synthesis_intensification.rfs_corn_impact_mlra_nathan_maps as  

SELECT
  mlra_5070_dissolved.mlra_id,  
  rfs_corn_impact_mlra.chg_corn_acres,
  (rfs_corn_impact_mlra.chg_corn_acres/mlra_5070_dissolved.acres)*100 as perc_chg_corn_acres_mlra, 
  rfs_corn_impact_mlra.perc_chg_corn * 100 as perc_chg_corn, 
  mlra_5070_dissolved.acres as mlra_acres, 
  st_area(wkb_geometry)*0.000247105 as qaqc_area_acres,
  mlra_5070_dissolved.acres - (st_area(wkb_geometry)*0.000247105) as qaqc_acres_diff,
  mlra_5070_dissolved.wkb_geometry
  
FROM 
  spatial.mlra_5070_dissolved, 
  synthesis_intensification.rfs_corn_impact_mlra
WHERE 
  mlra_5070_dissolved.mlra_id = rfs_corn_impact_mlra.mlra;
