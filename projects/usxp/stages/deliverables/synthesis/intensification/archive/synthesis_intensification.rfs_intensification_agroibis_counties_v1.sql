CREATE TABLE synthesis_intensification.rfs_intensification_agroibis_counties as  
 
SELECT 
  main.objectid, 
  main.unique_id, 
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
  main.nleach2007_2016mean_scen1 as nleach_cc, 
  main.nleach2007_2016mean_scen2 as nleach_ss, 
  main.nleach2007_2016mean_scen3 as nleach_cs, 
  main.nleach2007_2016mean_scen4 as nleach_ww, 
  main.nleach2007_2016mean_scen5 as nleach_cw, 
  main.pyield_2007_2016mean_scen1 as pyield_cc, 
  main.pyield_2007_2016mean_scen2 as pyield_ss, 
  main.pyield_2007_2016mean_scen3 as pyield_cs, 
  main.pyield_2007_2016mean_scen4 as pyield_ww, 
  main.pyield_2007_2016mean_scen5 as pyield_cw, 
  main.sedyield2007_2016mean_scen1 as sedyield_cc, 
  main.sedyield2007_2016mean_scen2 as sedyield_ss, 
  main.sedyield2007_2016mean_scen3 as sedyield_cs, 
  main.sedyield2007_2016mean_scen4 as sedyield_ww, 
  main.sedyield2007_2016mean_scen5 as sedyield_cw, 
  counties.atlas_st, 
  counties.st_abbrev,
  counties.atlas_stco, 
  counties.state_name, 
  counties.acres_calc,
  counties.geom 
FROM 
  synthesis_intensification.rfs_intensification_v3_agroibis as main, 
  spatial.counties
WHERE 
st_transform(main.shape,5070) && counties.geom AND
ST_Within(st_transform(main.shape,5070), counties.geom)
