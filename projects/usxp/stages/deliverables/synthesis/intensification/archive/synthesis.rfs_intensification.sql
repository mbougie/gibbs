create table synthesis.rfs_intensification_t2 as 
--t15 288
--t16 1811   (the link to the points is fuckinf shit up)
--t17 1574   (JOIN synthesis.rfs_intensification_agroibis_counties)
--t18 1574   (same as t17 but add bottom processing with eric's data --- choropleths are much diffrent than the original choropleth I created)
--t19 1811   (remove the connection to points to see if choropleths are the same)
--t20 1811   (remove the parathesis to see if choropleths are the same)
--t21 1574   (should be the same as t18)



----------------------------------------------------------------
--t1 1702   new sample datset
--t2 1702   add total column







SELECT 
count(a.objectid) as objectid_count,
a.atlas_stco,
--The change in acres of continuous corn in each county.
sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)) as acres_change_rot_cc, 

--The change in acres of continuous other crop in each county.
sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)) as acres_change_rot_ss, 

--The change in acres of corn-other in each county.
sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)) as acres_change_rot_ww,

--The change in acres of corn-other in each county.
sum(a.acres * (a.rot_prob_sc_rfs - a.rot_prob_sc_non_rfs)) as acres_change_rot_sc, 

--The change in acres of corn-other in each county.
sum(a.acres * (a.rot_prob_wc_rfs - a.rot_prob_wc_non_rfs)) as acres_change_rot_wc,  

--change of probabilities of continuous corn due to RFS  (county-level area-weighted average)
((sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)))/ SUM(a.acres)*100) as acres_change_rot_cc_awa, 

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
--((sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs))))


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


--acres_change_TOTAL_leach = acres_change_rot_CC_leach + acres_change_rot_SS_leach + acres_change_rot_WW_leach + acres_change_rot_CS_leach + acres_change_rot_CW_leach
((sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.leach_cc))) + ((sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.leach_ss))) + ((sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.leach_ww))) +
((sum(a.acres * (a.rot_prob_sc_rfs - a.rot_prob_sc_non_rfs)*a.leach_sc))) + ((sum(a.acres * (a.rot_prob_wc_rfs - a.rot_prob_wc_non_rfs)*a.leach_wc))) as acres_change_total_leach,


a.geom

--NOTE: the rfs_intesification dataset is the one with the bad objectid so need to link to this
FROM synthesis.rfs_intensification_pts_agroibis_counties as a 

--NOTE:Don't join these datasets for the bad processing (screws stuff up to mix good data with bad!!!!)
--JOIN merged.ksu_samples_final as b ON a.objectid = b.unique_id
--JOIN spatial.counties as c ON c.atlas_stco = b.fips

--NOTE: I had to attach the county information to synthesis.rfs_intensification_agroibis because of not being able to link to the correct objectids with bad objectids (sub-optimal temporary way of running the code)
--JOIN synthesis.rfs_intensification_agroibis_counties as d ON a.objectid = d.objectid

GROUP BY
  a.atlas_stco,
  a.acres_calc,
  a.geom

