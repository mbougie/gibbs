create table eric.s35_mtr3_4_id_pts_wgs84_formatted as  

SELECT 
  s35_mtr3_4_id_pts_wgs84.gridcode, 
  st_x(s35_mtr3_4_id_pts_wgs84.wkb_geometry) as lon, 
  st_y(s35_mtr3_4_id_pts_wgs84.wkb_geometry) as lat, 
  s35_mtr3_4_id_pts_wgs84.s35_mtr, 
  s35_mtr3_4_id_pts_wgs84.s35_fc, 
  s35_mtr3_4_id_pts_wgs84.s35_bfnc,
  COALESCE(s35_mtr3_4_id_pts_wgs84.s35_fc, s35_mtr3_4_id_pts_wgs84.s35_bfnc) as fc_bfnc,
  s35_mtr3_4_id_pts_wgs84.s35_bfc, 
  s35_mtr3_4_id_pts_wgs84.s35_fnc,
  COALESCE(s35_mtr3_4_id_pts_wgs84.s35_bfc, s35_mtr3_4_id_pts_wgs84.s35_fnc) as bfc_fnc,
  --lu_1.veg_type_ext as vegScen1, 
  --lu_2.veg_type_ext as vegScen2, 
  s35_mtr3_4_id_pts_wgs84.wkb_geometry as geom
FROM 
 -- eric_lookup.lookup as lu_1, 
  eric.s35_mtr3_4_id_pts_wgs84 
 -- eric_lookup.lookup as lu_2
--WHERE 
 -- lu_1.cdl = COALESCE(s35_mtr3_4_id_pts_wgs84.s35_fc, s35_mtr3_4_id_pts_wgs84.s35_bfnc) AND
  --lu_2.cdl = COALESCE(s35_mtr3_4_id_pts_wgs84.s35_bfc, s35_mtr3_4_id_pts_wgs84.s35_fnc);
