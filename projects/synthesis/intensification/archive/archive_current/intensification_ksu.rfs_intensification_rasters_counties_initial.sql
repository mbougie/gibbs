Query returned successfully: 3602949 rows affected, 227427 ms execution time.

CREATE TABLE intensification_ksu.rfs_intensification_rasters_counties as  
SELECT
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
rasters."Nleach_2007.2016median_Scen1" as nleach_cc,
rasters."Nleach_2007.2016median_Scen2" as nleach_ss,
rasters."Nleach_2007.2016median_Scen3" as nleach_cs,
rasters."Nleach_2007.2016median_Scen4" as nleach_ww,
rasters."Nleach_2007.2016median_Scen5" as nleach_cw,
--"Nleach_2007.2016median_Scen6" ,
rasters."Pyield_2007.2016median_Scen1" as pyield_cc,
rasters."Pyield_2007.2016median_Scen2" as pyield_ss,
rasters."Pyield_2007.2016median_Scen3" as pyield_cs,
rasters."Pyield_2007.2016median_Scen4" as pyield_ww,
rasters."Pyield_2007.2016median_Scen5" as pyield_cw,
--"Pyield_2007.2016median_Scen6" ,
rasters."SEDyield_2007.2016median_Scen1" as sedyield_cc,
rasters."SEDyield_2007.2016median_Scen2" as sedyield_ss,
rasters."SEDyield_2007.2016median_Scen3" as sedyield_cs,
rasters."SEDyield_2007.2016median_Scen4" as sedyield_ww,
rasters."SEDyield_2007.2016median_Scen5" as sedyield_cw,
--"SEDyield_2007.2016median_Scen6" 
counties.atlas_st, 
counties.st_abbrev,
counties.atlas_stco, 
counties.state_name, 
counties.acres_calc,
counties.geom 
FROM 
intensification_ksu.rfs_intensification as main INNER JOIN intensification_ksu.rfs_intensification_rasters as rasters  USING(unique_id),
spatial.counties
WHERE 
st_transform(main.geom,5070) && counties.geom AND
ST_Within(st_transform(main.geom,5070), counties.geom)








