--CREATE TABLE synthesis.rfs_intensification_qaqc_nongrouped as

SELECT 
 a.rot_prob_cc_rfs,   --prob of cont corn w/ rfs
 a.rot_prob_cc_non_rfs, --prob of cont corn w/out rfs
 a.acres,  --acres per patch
 a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs as diff,  -- diff in probabilities
 a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs) as diff_weight,  --change of probabilities of continuous corn due to RFS
 b.fips,
 c.geom
FROM synthesis.rfs_intensification_limit_10000 as a 
JOIN merged.ksu_samples_final as b
ON
  a.objectid = b.unique_id
JOIN spatial.counties as c
ON 
  c.atlas_stco = b.fips

where fips = '19095'


