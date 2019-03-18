--CREATE TABLE eric.s35_mtr3_4_id_pts_wgs84_differences AS

SELECT 
  a.gridcode,
  a.lon,
  a.lat,
  a.mtr,
  a.fc,
  a.bfnc,
  a.fc_bfnc,
  a.bfc,
  a.fnc,
  a.bfc_fnc,
  a.vegscen1,
  a.vegscen2,
  b.gridcode as gridcode_maj,
  b.lon as lon_maj,
  b.lat as lat_maj,
  b.mtr as mtr_maj,
  b.fc as fc_maj,
  b.bfnc as bfnc_maj,
  b.fc_bfnc as fc_bfnc_maj,
  b.bfc as bfc_maj,
  b.fnc as fnc_maj,
  b.bfc_fnc as bfc_fnc_maj,
  b.vegscen1 as vegscen1_maj,
  b.vegscen2 as vegscen2_maj,
  a.geom
FROM 
  eric.s35_mtr3_4_id_pts_wgs84_formatted a FULL OUTER JOIN
  eric.s35_mtr3_4_id_pts_wgs84_formatted_majority b
ON 
  --see all records that match on the rpimary key [543922]
  --a.gridcode = b.gridcode

  --see all the records that match on gridcode and lat/lon fields [543922]  --assume that have the same lat/lon values
  --a.gridcode = b.gridcode AND a.lon = b.lon AND a.lat = b.lat

  --blah blah [102,802]
  --a.gridcode = b.gridcode AND a.lon = b.lon AND a.lat = b.lat
  --WHERE a.fc_bfnc <> b.fc_bfnc

  --blah blah [70,370]
  --a.gridcode = b.gridcode AND a.lon = b.lon AND a.lat = b.lat
  --WHERE a.bfc_fnc <> b.bfc_fnc

  ---blah blah [155217]  --OR--  
  a.gridcode = b.gridcode AND a.lon = b.lon AND a.lat = b.lat
  WHERE a.fc_bfnc <> b.fc_bfnc OR a.bfc_fnc <> b.bfc_fnc

   ---blah blah [17955] --AND--
  --a.gridcode = b.gridcode AND a.lon = b.lon AND a.lat = b.lat
  --WHERE a.fc_bfnc <> b.fc_bfnc AND a.bfc_fnc <> b.bfc_fnc

  -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  --NOTE 102,802 + 70,370 = 173,172 but only got 155217 when used the OR operator (a diffrence of 17955)  ---so this means there are 17955 records with changes in BOTH columns
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
