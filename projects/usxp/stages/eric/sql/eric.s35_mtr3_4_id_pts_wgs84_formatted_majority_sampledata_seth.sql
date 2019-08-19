create table eric.s35_mtr3_4_id_pts_wgs84_formatted_majority_sampledata_seth as 
----Description: This dataset contains all records that were excluded from eric.s35_mtr3_4_id_pts_wgs84_formatted_majority_sampledata dataset because either mlra_id OR atlas_stco was NULL.
--Query returned successfully: 543926 rows affected, 52444 ms execution time.
SELECT 
  merged_table.id as gridcode, 
  st_x((ST_Dump(wkb_geometry)).geom) as lon,
  st_y((ST_Dump(wkb_geometry)).geom) as lat, 
  merged_table.link as mtr, 
  --s35_fc.majority as fc, 
  --s35_bfnc.majority as bfnc,
  COALESCE(s35_fc.majority, s35_bfnc.majority) as fc_bfnc, 
  --s35_bfc.majority as bfc, 
  --s35_fnc.majority as fnc,
  COALESCE(s35_bfc.majority, s35_fnc.majority) as bfc_fnc,
  lu_1.veg_type_ext as vegScen1, 
  lu_2.veg_type_ext as vegScen2,
  atlas_stco,
  mlra_id,
  huc10,
  huc12,
  merged_table.count * 0.222395 as acres
  --s35_mtr3_4_id_pts_wgs84.wkb_geometry as geom
FROM 
  eric.merged_table 
  LEFT OUTER JOIN eric.s35_bfc
  ON s35_bfc.value = merged_table.id 
  LEFT OUTER JOIN eric.s35_bfnc
  ON s35_bfnc.value = merged_table.id
  LEFT OUTER JOIN eric.s35_fc
  ON s35_fc.value = merged_table.id 
  LEFT OUTER JOIN eric.s35_fnc
  ON s35_fnc.value = merged_table.id
  INNER JOIN eric_lookup.lookup as lu_1 
  ON  lu_1.cdl = COALESCE(s35_fc.majority, s35_bfnc.majority)
  INNER JOIN eric_lookup.lookup as lu_2
  ON  lu_2.cdl = COALESCE(s35_bfc.majority, s35_fnc.majority)
  FULL OUTER JOIN eric.s35_mtr3_4_id_pts_wgs84
  ON s35_mtr3_4_id_pts_wgs84.gridcode = merged_table.id 
  FULL OUTER JOIN eric.counties
  ON counties.gridcode = merged_table.id 
  FULL OUTER JOIN eric.mlra
  ON mlra.gridcode = merged_table.id 
  FULL OUTER JOIN eric.wbdhu10
  ON wbdhu10.gridcode = merged_table.id 
  FULL OUTER JOIN eric.wbdhu12
  ON wbdhu12.gridcode = merged_table.id 

---WHERE 


