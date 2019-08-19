----Description: This dataset shows the records that were excluded from eric.s35_mtr3_4_id_pts_wgs84_formatted_majority_sampledata dataset because either mlra_id OR atlas_stco was NULL.
----543926 - 543559 = 367
----Query returned successfully: 367 rows affected, 573 ms execution time.

CREATE TABLE eric.s35_mtr3_4_id_pts_wgs84_formatted_majority_sampledata_qaqc as

SELECT 
  * 
FROM 
  eric.s35_mtr3_4_id_pts_wgs84_formatted_majority_sampledata_seth
where mlra_id is null OR atlas_stco is NULL
