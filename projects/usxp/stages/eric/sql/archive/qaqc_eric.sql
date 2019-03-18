

---qaqc to see if the binary column in lookup is all ones
SELECT 
  fc_bfnc,
  count(fc_bfnc),
  b
FROM 
  eric.s35_mtr3_4_id_pts_wgs84_formatted INNER JOIN misc.lookup_cdl ON fc_bfnc = value
group by fc_bfnc,b
order by fc_bfnc



---qaqc to see if the binary column in lookup is all zeros
SELECT 
  bfc_fnc,
  count(bfc_fnc),
  b
FROM 
  eric.s35_mtr3_4_id_pts_wgs84_formatted INNER JOIN misc.lookup_cdl ON bfc_fnc = value
group by bfc_fnc,b
order by bfc_fnc