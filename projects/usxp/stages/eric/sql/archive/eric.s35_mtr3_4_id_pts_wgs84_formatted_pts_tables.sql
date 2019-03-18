create table eric.s35_mtr3_4_id_pts_wgs84_formatted_pts_extra as  

--EXPLAIN
SELECT 
  points.gridcode, 
  st_x((ST_Dump(points.wkb_geometry)).geom) as lon,
  st_y((ST_Dump(points.wkb_geometry)).geom) as lat, 
  points.s35_mtr as mtr, 
  points.s35_fc as fc, 
  points.s35_bfnc as bfnc,
  COALESCE(points.s35_fc, points.s35_bfnc) as fc_bfnc,
  points.s35_bfc as bfc, 
  points.s35_fnc as fnc,
  COALESCE(points.s35_bfc, points.s35_fnc) as bfc_fnc,
  lu_1.veg_type_ext as vegScen1, 
  lu_2.veg_type_ext as vegScen2,
  counties.atlas_stco,
  mlra.mlra_id,
  wbdhu10.huc10,
  wbdhu12.huc12
  --points.wkb_geometry

FROM 
eric_lookup.lookup as lu_1,
eric.s35_mtr3_4_id_pts_wgs84 as points,
eric_lookup.lookup as lu_2,
eric.counties,
eric.mlra,
eric.wbdhu10,
eric.wbdhu12


WHERE 
lu_1.cdl = COALESCE(points.s35_fc, points.s35_bfnc) AND
lu_2.cdl = COALESCE(points.s35_bfc, points.s35_fnc) AND
points.gridcode=counties.gridcode AND
points.gridcode=mlra.gridcode AND
points.gridcode=wbdhu10.gridcode AND
points.gridcode=wbdhu12.gridcode
  







