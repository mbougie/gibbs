CREATE TABLE synthesis_intensification.rfs_intensification_results_states_v2 as

SELECT 
  sum(rfs_intensification_results_counties.objectid_count) as objectid_count, 
  counties.state_name, 
  sum(rfs_intensification_results_counties.acres) as acres, 
  sum(rfs_intensification_results_counties.hectares) as hectares, 
  sum(rfs_intensification_results_counties.km2) as km2, 
  sum(rfs_intensification_results_counties.acres_change_rot_cc) as acres_change_rot_cc, 
  --rfs_intensification_results_counties.acres_change_rot_ss, 
  --rfs_intensification_results_counties.acres_change_rot_ww, 
  --rfs_intensification_results_counties.acres_change_rot_cs, 
  --rfs_intensification_results_counties.acres_change_rot_cw, 
  sum(rfs_intensification_results_counties.acres_change_rot_oo) as acres_change_rot_oo, 
  sum(rfs_intensification_results_counties.acres_change_rot_co) as acres_change_rot_co, 
  sum(rfs_intensification_results_counties.acres_change_rot_cc_awa) as acres_change_rot_cc_awa, 
  --rfs_intensification_results_counties.acres_change_rot_ss_awa, 
  --rfs_intensification_results_counties.acres_change_rot_ww_awa, 
  --rfs_intensification_results_counties.acres_change_rot_cs_awa, 
  --rfs_intensification_results_counties.acres_change_rot_cw_awa, 
  sum(rfs_intensification_results_counties.acres_change_rot_oo_awa) as acres_change_rot_oo_awa, 
  sum(rfs_intensification_results_counties.acres_change_rot_co_awa) as acres_change_rot_co_awa, 
  sum(rfs_intensification_results_counties.hectares_change_rot_cc_napp) as hectares_change_rot_cc_napp, 
  sum(rfs_intensification_results_counties.tons_co2e_change_rot_cc_n2o_mean) as tons_co2e_change_rot_cc_n2o_mean, 
  --rfs_intensification_results_counties.tons_co2e_change_rot_cc_n2o_p025, 
  --rfs_intensification_results_counties.tons_co2e_change_rot_cc_n2o_p975, 
  sum(rfs_intensification_results_counties.hectares_change_rot_oo_napp) as hectares_change_rot_oo_napp, 
  sum(rfs_intensification_results_counties.tons_co2e_change_rot_oo_n2o_mean) as tons_co2e_change_rot_oo_n2o_mean, 
  --rfs_intensification_results_counties.tons_co2e_change_rot_oo_n2o_p025, 
  --rfs_intensification_results_counties.tons_co2e_change_rot_oo_n2o_p975, 
  sum(rfs_intensification_results_counties.hectares_change_rot_co_napp) as hectares_change_rot_co_napp, 
  sum(rfs_intensification_results_counties.tons_co2e_change_rot_co_n2o_mean) as tons_co2e_change_rot_co_n2o_mean, 
  --rfs_intensification_results_counties.tons_co2e_change_rot_co_n2o_p025, 
  --rfs_intensification_results_counties.tons_co2e_change_rot_co_n2o_p975, 
  sum(rfs_intensification_results_counties.hectares_change_total_napp) as hectares_change_total_napp, 
  sum(rfs_intensification_results_counties.hectares_change_total_n2o_mean) as hectares_change_total_n2o_mean, 
  sum(rfs_intensification_results_counties.tons_co2e_change_total_n2o_mean) as tons_co2e_change_total_n2o_mean 
  --rfs_intensification_results_counties.tons_co2e_change_total_n2o_p025, 
  --rfs_intensification_results_counties.tons_co2e_change_total_n2o_p975, 
  --rfs_intensification_results_counties.geom
FROM 
  synthesis_intensification.rfs_intensification_results_counties
INNER JOIN
spatial.counties USING(atlas_stco) 

GROUP BY state_name
