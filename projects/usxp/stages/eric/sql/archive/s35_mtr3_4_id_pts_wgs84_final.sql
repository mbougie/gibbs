create table eric_s35_t2.s35_mtr3_4_id_pts_wgs84_final as  

SELECT 
  a.gridcode, 
  a.lat, 
  a.lon, 
  a.mtr, 
  a.fc_bfnc, 
  a.bfc_fnc, 
  b.veg_type_ext as vegScen1, 
  c.veg_type_ext as vegScen2,
  a.geom
FROM 
  eric_s35_t2.s35_mtr3_4_id_pts_wgs84 as a, 
  eric_lookup.lookup as b, 
  eric_lookup.lookup as c
WHERE 
  b.cdl = a.fc_bfnc AND
  c.cdl = a.bfc_fnc;
