/*update eric.s35_mtr3_4_id_pts_wgs84 set fc_bfnc = fc;
update eric.s35_mtr3_4_id_pts_wgs84 set fc_bfnc = bfnc where fc_bfnc = 0;



update eric.s35_mtr3_4_id_pts_wgs84 set bfc_fnc = bfc;
update eric.s35_mtr3_4_id_pts_wgs84 set bfc_fnc = fnc where bfc_fnc = 0;*/



ALTER TABLE eric.s35_mtr3_4_id_pts_wgs84 ADD COLUMN lon double precision;
update eric.s35_mtr3_4_id_pts_wgs84 set lon = ST_X(geom);
ALTER TABLE eric.s35_mtr3_4_id_pts_wgs84 ADD COLUMN lat double precision;
update eric.s35_mtr3_4_id_pts_wgs84 set lat = ST_Y(geom)