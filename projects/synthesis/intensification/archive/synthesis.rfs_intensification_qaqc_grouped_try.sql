CREATE TABLE synthesis.rfs_intensification_qaqc_grouped_new_try_t2 as

--note 1811 counties in all--------
--note 288 counties with erics data attached--------
--note 580 counties with erics data attached--------

SELECT 
count(a.objectid) as objectid_count,
b.fips,
--The change in acres of continuous corn in each county.
((sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)))) as acres_change_rot_cc, 

--The change in acres of continuous other crop in each county.
((sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)))) as acres_change_rot_ss, 

--The change in acres of corn-other in each county.
((sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)))) as acres_change_rot_ww,

--The change in acres of corn-other in each county.
((sum(a.acres * (a.rot_prob_sc_rfs - a.rot_prob_sc_non_rfs)))) as acres_change_rot_sc, 

--The change in acres of corn-other in each county.
((sum(a.acres * (a.rot_prob_wc_rfs - a.rot_prob_wc_non_rfs)))) as acres_change_rot_wc,  

--change of probabilities of continuous corn due to RFS  (county-level area-weighted average)
((sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs))))/ SUM(a.acres)*100 as acres_change_rot_cc_awa, 

--change of probabilities of other crop due to RFS  (county-level area-weighted average)
((sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs))))/ SUM(a.acres)*100 as acres_change_rot_ss_awa, 

--change of probabilities of corn-other due to RFS  (county-level area-weighted average)
((sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs))))/ SUM(a.acres)*100 as acres_change_rot_ww_awa,

--change of probabilities of corn-other due to RFS  (county-level area-weighted average)
((sum(a.acres * (a.rot_prob_sc_rfs - a.rot_prob_sc_non_rfs))))/ SUM(a.acres)*100 as acres_change_rot_sc_awa, 

--change of probabilities of corn-other due to RFS  (county-level area-weighted average)
((sum(a.acres * (a.rot_prob_wc_rfs - a.rot_prob_wc_non_rfs))))/ SUM(a.acres)*100 as acres_change_rot_wc_awa,  

--Increased phosphorus runoff from increased continuous corn in a county.
--((sum(a.acres * (a.rot_prob_cc_rfs - A.NOT_RFS)) * P_XX), 

--Increased xxxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.leach_cc)) as acres_change_rot_cc_leach,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.pyield_cc)) as acres_change_rot_cc_pyield,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sedyield_cc)) as acres_change_rot_cc_sedyield,


--Increased xxxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.leach_ss)) as acres_change_rot_ss_leach,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.pyield_ss)) as acres_change_rot_ss_pyield,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.sedyield_ss)) as acres_change_rot_ss_sedyield,


--Increased xxxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.leach_ww)) as acres_change_rot_ww_leach,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.pyield_ww)) as acres_change_rot_ww_pyield,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.sedyield_ww)) as acres_change_rot_ww_sedyield,


--Increased xxxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_sc_rfs - a.rot_prob_sc_non_rfs)*a.leach_sc)) as acres_change_rot_sc_leach,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_sc_rfs - a.rot_prob_sc_non_rfs)*a.pyield_sc)) as acres_change_rot_sc_pyield,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_sc_rfs - a.rot_prob_sc_non_rfs)*a.sedyield_sc)) as acres_change_rot_sc_sedyield,


--Increased xxxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_wc_rfs - a.rot_prob_wc_non_rfs)*a.leach_wc)) as acres_change_rot_wc_leach,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_wc_rfs - a.rot_prob_wc_non_rfs)*a.pyield_wc)) as acres_change_rot_wc_pyield,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_wc_rfs - a.rot_prob_wc_non_rfs)*a.sedyield_wc)) as acres_change_rot_wc_sedyield,

c.geom

  
FROM synthesis.rfs_intensification_agroibis_initial_t3 as a 
JOIN merged.ksu_samples_final as b ON a.objectid = b.unique_id
JOIN spatial.counties as c ON c.atlas_stco = b.fips
--JOIN synthesis.rfs_intensification_agroibis as d ON a.objectid = d.objectid

GROUP BY
  b.fips,
  c.acres_calc,
  c.geom

