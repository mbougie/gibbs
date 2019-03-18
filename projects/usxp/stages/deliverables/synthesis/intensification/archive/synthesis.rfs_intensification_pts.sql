create table synthesis.rfs_intensification_pts as 

SELECT 
  rfs_intensification.index, 
  rfs_intensification.objectid, 
  rfs_intensification.lng, 
  rfs_intensification.lat, 
  rfs_intensification.acres, 
  rfs_intensification.rot_prob_cc_rfs, 
  rfs_intensification.rot_prob_ss_rfs, 
  rfs_intensification.rot_prob_ww_rfs, 
  rfs_intensification.rot_prob_sc_rfs, 
  rfs_intensification.rot_prob_wc_rfs, 
  rfs_intensification.rot_prob_cc_non_rfs, 
  rfs_intensification.rot_prob_ss_non_rfs, 
  rfs_intensification.rot_prob_ww_non_rfs, 
  rfs_intensification.rot_prob_sc_non_rfs, 
  rfs_intensification.rot_prob_wc_non_rfs
FROM 
  synthesis_initial.rfs_intensification;
  --limit 10;


SELECT AddGeometryColumn ('synthesis','rfs_intensification_pts','geom',4152,'POINT',2);
UPDATE synthesis.rfs_intensification_pts SET geom = st_SetSrid(ST_MakePoint(lng, lat), 4152);
