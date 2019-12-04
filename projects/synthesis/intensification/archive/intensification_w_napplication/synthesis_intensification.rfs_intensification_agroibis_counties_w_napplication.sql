---attach the county info to the main table


CREATE TABLE synthesis_intensification.rfs_intensification_agroibis_counties as  
 
SELECT 
  main.objectid, 
  main.u_id as unique_id, 
  main.mlra, 
  main.lon, 
  main.lat, 
  main.acres, 
  
  main.rot_prob_cc_rfs, 
  main.rot_prob_ss_rfs, 
  main.rot_prob_ww_rfs, 
  main.rot_prob_sc_rfs as rot_prob_cs_rfs, 
  main.rot_prob_wc_rfs as rot_prob_cw_rfs, 
  main.rot_prob_cc_non_rfs, 
  main.rot_prob_ss_non_rfs, 
  main.rot_prob_ww_non_rfs, 
  main.rot_prob_sc_non_rfs as rot_prob_cs_non_rfs, 
  main.rot_prob_wc_non_rfs as rot_prob_cw_non_rfs,
   
  main.napplication2007_2016mean_scen1 as napplication_cc, 
  main.napplication2007_2016mean_scen2 as napplication_ss, 
  main.napplication2007_2016mean_scen3 as napplication_cs, 
  main.napplication2007_2016mean_scen4 as napplication_ww, 
  main.napplication2007_2016mean_scen5 as napplication_cw,
  
  n2o.scen1_mean::double precision as n2o_mean_cc,
  n2o.scen2_mean::double precision as n2o_mean_ss,
  n2o.scen3_mean::double precision as n2o_mean_cs,
  n2o.scen4_mean::double precision as n2o_mean_ww,
  n2o.scen5_mean::double precision as n2o_mean_cw,

  n2o.scen1_p025::double precision as n2o_p025_cc,
  n2o.scen2_p025::double precision as n2o_p025_ss,
  n2o.scen3_p025::double precision as n2o_p025_cs,
  n2o.scen4_p025::double precision as n2o_p025_ww,
  n2o.scen5_p025::double precision as n2o_p025_cw,

  n2o.scen1_p975::double precision as n2o_p975_cc,
  n2o.scen2_p975::double precision as n2o_p975_ss,
  n2o.scen3_p975::double precision as n2o_p975_cs,
  n2o.scen4_p975::double precision as n2o_p975_ww,
  n2o.scen5_p975::double precision as n2o_p975_cw,
  
  counties.atlas_st, 
  counties.st_abbrev,
  counties.atlas_stco, 
  counties.state_name, 
  counties.acres_calc as acres_county,
  counties.geom 
FROM 
  synthesis_intensification.rfs_intensification_v3_agroibis as main,  ---this is the main dataset that extracts nathan's points with eric's rasters
  synthesis_intensification.rfs_intensification_v3_n2o as n2o,  --seth's data
  spatial.counties

-- 
WHERE 
st_transform(main.shape,5070) && counties.geom AND  ----spatial subset 
ST_Within(st_transform(main.shape,5070), counties.geom) AND
main.u_id = uniqu_d::integer

--limit 100

