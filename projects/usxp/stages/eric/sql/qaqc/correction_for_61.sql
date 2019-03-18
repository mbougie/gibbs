--------------------------------------------------
---- query to visualize --------------------------
--------------------------------------------------

SELECT
a.mtr,
a.gridcode,
a.fc_bfnc as fc_bfnc_initial,
b.fc_bfnc, 
a.veg_scen1 as vegscen1_initial,
b.vegscen1
FROM 
eric_s35_delivered_w_61.s35_mtr3_4_id_pts_wgs84_formatted as a JOIN
eric.s35_mtr3_4_id_pts_wgs84_formatted as b 
ON
a.gridcode = b.gridcode
-----cond_0.0(get all records(both mtr=3 and mtr=4) where a.fc_bfnc = 61 [47,896])
WHERE a.fc_bfnc = 61

-----cond_0.2(get all records where a.fc_bfnc = 61 and a.mtr=3 [32,709])
--WHERE a.fc_bfnc = 61 and a.mtr=3

-----cond_0.3(get all records where a.fc_bfnc = 61 and a.mtr=4 [15,187])
--WHERE  b.fc_bfnc = 61 and a.mtr=4

------Note: 32,709 + 15,187 = 47,896



-----cond_1(get all records where diffrence in vegscen1 between two tables [17,420])
--WHERE a.veg_scen1 <> b.vegscen1

-----cond_2(check to see all the records that are diffrence for fc_bfnc that are not equal to 61 <---this is weird!?! [2,599])
--WHERE a.fc_bfnc <> b.fc_bfnc AND a.fc_bfnc <> 61

-----cond_3(get all records where diffrence in fc_bfnc between two tables [35,308])
--WHERE a.fc_bfnc <> b.fc_bfnc

------Note: 32,709 + 2599 = 35,308--------------------------------------------

-----cond_4(get all records where diffrence in bfc_fnc between two tables [0])
--WHERE a.bfc_fnc <> b.bfc_fnc

-----cond_5(get all records where diffrence in veg_scen2 between two tables [0])
--WHERE a.veg_scen2 <> b.vegscen2


ORDER BY fc_bfnc



-------------------------------------------------
----  polished query ----------------------------
-------------------------------------------------
--CREATE TABLE eric.s35_mtr3_4_id_wgs84 AS
SELECT
b.gridcode,
b.lon,
b.lat,
b.mtr,
b.fc_bfnc,
b.bfc_fnc,
b.vegscen1,
b.vegscen2,
b.geom

FROM 
eric_s35_delivered_w_61.s35_mtr3_4_id_pts_wgs84_formatted as a JOIN
eric.s35_mtr3_4_id_pts_wgs84_formatted as b 
ON
a.gridcode = b.gridcode
-----cond_1(get all records where diffrence in veg_scen1 between two tables [17,420])
--WHERE a.veg_scen1 <> b.vegscen1

-----cond_2(get all records where diffrence in fc_bfnc between two tables [35,308])    <----- optimal condition!!!!!
WHERE a.fc_bfnc <> b.fc_bfnc

-----cond_3(check to see all the records that are diffrence for fc_bfnc that are not equal to 61 <---this is weird!?! [2,599])
--WHERE a.fc_bfnc <> b.fc_bfnc AND a.fc_bfnc <> 61

-----cond_4(get all records where diffrence in bfc_fnc between two tables [0])
--WHERE a.bfc_fnc <> b.bfc_fnc

-----cond_5(get all records where diffrence in veg_scen2 between two tables [0])
--WHERE a.veg_scen2 <> b.vegscen2