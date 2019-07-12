create table synthesis.rfs_intensification_agroibis_initial_t4 as 


SELECT 
  rfs_intensification_agroibis_initial.objectid, 
  lng, 
  lat,
  acres,
  rot_prob_cc_rfs,
  rot_prob_ss_rfs,
  rot_prob_ww_rfs,
  rot_prob_sc_rfs,
  rot_prob_wc_rfs,
  rot_prob_cc_non_rfs,
  rot_prob_ss_non_rfs,
  rot_prob_ww_non_rfs,
  rot_prob_sc_non_rfs,
  rot_prob_wc_non_rfs,
  COALESCE(rfs_intensification_agroibis_initial.nleach2007_2016mean_scen1,0.00001) as leach_cc, 
  COALESCE(rfs_intensification_agroibis_initial.nleach2007_2016mean_scen2,0.00001) as leach_ss, 
  COALESCE(rfs_intensification_agroibis_initial.nleach2007_2016mean_scen3,0.00001) as leach_sc, 
  COALESCE(rfs_intensification_agroibis_initial.nleach2007_2016mean_scen4,0.00001) as leach_ww, 
  COALESCE(rfs_intensification_agroibis_initial.nleach2007_2016mean_scen5,0.00001) as leach_wc, 
  COALESCE(rfs_intensification_agroibis_initial.pyield_2007_2016mean_scen1,0.00001) as pyield_cc, 
  COALESCE(rfs_intensification_agroibis_initial.pyield_2007_2016mean_scen2,0.00001) as pyield_ss, 
  COALESCE(rfs_intensification_agroibis_initial.pyield_2007_2016mean_scen3,0.00001) as pyield_sc, 
  COALESCE(rfs_intensification_agroibis_initial.pyield_2007_2016mean_scen4,0.00001) as pyield_ww, 
  COALESCE(rfs_intensification_agroibis_initial.pyield_2007_2016mean_scen5,0.00001) as pyield_wc, 
  COALESCE(rfs_intensification_agroibis_initial.sedyield2007_2016mean_scen1,0.00001) as sedyield_cc, 
  COALESCE(rfs_intensification_agroibis_initial.sedyield2007_2016mean_scen2,0.00001) as sedyield_ss, 
  COALESCE(rfs_intensification_agroibis_initial.sedyield2007_2016mean_scen3,0.00001) as sedyield_sc, 
  COALESCE(rfs_intensification_agroibis_initial.sedyield2007_2016mean_scen4,0.00001) as sedyield_ww, 
  COALESCE(rfs_intensification_agroibis_initial.sedyield2007_2016mean_scen5,0.00001) as sedyield_wc, 
  rfs_intensification_agroibis_initial.shape as geom
FROM 
  synthesis.rfs_intensification_agroibis_initial


--scen01: continuous corn cc 
--scen02: continuous soy ss
--scen03: corn-soy rotation sc
--scen04: continuous wheat ww
--scen05: corn-wheat rotation wc
