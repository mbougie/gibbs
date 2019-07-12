CREATE TABLE synthesis.rfs_intensification_poly as

--note 288 counties are 

SELECT 
  ((sum(a.acres * a.rot_prob_cc_rfs))/c.acres_calc)*100 as rot_prob_cc_rfs, 
  ((sum(a.acres * a.rot_prob_ss_rfs))/c.acres_calc)*100 as rot_prob_ss_rfs,
  ((sum(a.acres * a.rot_prob_ww_rfs))/c.acres_calc)*100 as rot_prob_ww_rfs,
  ((sum(a.acres * a.rot_prob_sc_rfs))/c.acres_calc)*100 as rot_prob_sc_rfs,
  ((sum(a.acres * a.rot_prob_wc_rfs))/c.acres_calc)*100 as rot_prob_wc_rfs,
  ((sum(a.acres * a.rot_prob_cc_non_rfs))/c.acres_calc)*100 as rot_prob_cc_non_rfs,
  ((sum(a.acres * a.rot_prob_ss_non_rfs))/c.acres_calc)*100 as rot_prob_ss_non_rfs,
  ((sum(a.acres * a.rot_prob_ww_non_rfs))/c.acres_calc)*100 as rot_prob_ww_non_rfs,
  ((sum(a.acres * a.rot_prob_sc_non_rfs))/c.acres_calc)*100 as rot_prob_sc_non_rfs,
  ((sum(a.acres * a.rot_prob_wc_non_rfs))/c.acres_calc)*100 as rot_prob_wc_non_rfs,
  count(a.objectid) as objectid_count,
  b.fips,
  c.geom
FROM synthesis.rfs_intensification as a 
JOIN merged.ksu_samples_final as b
ON
  a.objectid = b.unique_id
JOIN spatial.counties as c
ON 
  c.atlas_stco = b.fips
GROUP BY
  b.fips,
  c.acres_calc,
  c.geom

