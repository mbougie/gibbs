---Query returned successfully: 3602949 rows affected, 179106 ms execution time.

create table intensification.rfs_intensification_w_counties as

SELECT
main.unique_id, 
main.mlra, 
main.lon, 
main.lat, 
main.acres, 
main.rot_prob_cc_rfs, 
main.rot_prob_ss_rfs, 
main.rot_prob_ww_rfs, 
main.rot_prob_sc_rfs,
main.rot_prob_wc_rfs,
main.rot_prob_cc_non_rfs, 
main.rot_prob_ss_non_rfs, 
main.rot_prob_ww_non_rfs, 
main.rot_prob_sc_non_rfs,
main.rot_prob_wc_non_rfs,

counties.atlas_st, 
counties.st_abbrev,
counties.atlas_stco,
counties.fips,
counties.state_name, 
counties.acres_calc,
st_area(counties.geom) as m2,
counties.geom 
FROM 
intensification_v1.rfs_intensification as main,
spatial.counties
WHERE 
st_transform(main.geom,5070) && counties.geom AND
ST_Within(st_transform(main.geom,5070), counties.geom)