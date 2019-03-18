create table eric.s35_mtr3_4_id_pts_wgs84_formatted_pts as  

SELECT 
  s35_mtr3_4_id_pts_wgs84.gridcode, 
  st_x((ST_Dump(wkb_geometry)).geom) as lon,
  st_y((ST_Dump(wkb_geometry)).geom) as lat, 
  s35_mtr3_4_id_pts_wgs84.s35_mtr as mtr, 
  s35_mtr3_4_id_pts_wgs84.s35_fc as fc, 
  s35_mtr3_4_id_pts_wgs84.s35_bfnc as bfnc,
  COALESCE(s35_mtr3_4_id_pts_wgs84.s35_fc, s35_mtr3_4_id_pts_wgs84.s35_bfnc) as fc_bfnc,
  s35_mtr3_4_id_pts_wgs84.s35_bfc as bfc, 
  s35_mtr3_4_id_pts_wgs84.s35_fnc as fnc,
  COALESCE(s35_mtr3_4_id_pts_wgs84.s35_bfc, s35_mtr3_4_id_pts_wgs84.s35_fnc) as bfc_fnc,
  lu_1.veg_type_ext as vegScen1, 
  lu_2.veg_type_ext as vegScen2, 
  s35_mtr3_4_id_pts_wgs84.wkb_geometry as geom
FROM 
  eric_lookup.lookup as lu_1, 
  eric.s35_mtr3_4_id_pts_wgs84, 
  eric_lookup.lookup as lu_2
WHERE 
   lu_1.cdl = COALESCE(s35_mtr3_4_id_pts_wgs84.s35_fc, s35_mtr3_4_id_pts_wgs84.s35_bfnc) AND
  lu_2.cdl = COALESCE(s35_mtr3_4_id_pts_wgs84.s35_bfc, s35_mtr3_4_id_pts_wgs84.s35_fnc)
