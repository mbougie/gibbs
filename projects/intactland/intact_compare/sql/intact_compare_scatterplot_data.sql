SELECT 
	intact_compare_region_refined_hist_counties.label, 
	intact_compare.intactland_15_re as intact_clu, 
	intact_compare.nlcd_intact_01to as intact_nlcd, 
	intact_compare.intact_pete_rast as intact_sdsu,
	counties_102003.state_name as state,
	counties_102003.atlas_name as county,
	intact_compare_region_refined_hist_counties.atlas_stco, 
	intact_compare_region_refined_hist_counties.count, 
	intact_compare_region_refined_hist_counties.acres 

FROM 
  intact_compare.intact_compare, 
  intact_compare.intact_compare_region_refined_hist_counties,
  spatial.counties_102003
WHERE 
  intact_compare.value = intact_compare_region_refined_hist_counties.label::integer AND counties_102003.atlas_stco = intact_compare_region_refined_hist_counties.atlas_stco
