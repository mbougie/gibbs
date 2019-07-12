create table synthesis_intensification.rfs_intensification_results_counties as 

SELECT 
count(a.objectid) as objectid_count,
a.atlas_stco,

----------- raw --------------------------------------------------------------------
/*a.rot_prob_cc_rfs,
a.rot_prob_cc_non_rfs,
a.rot_prob_ss_rfs,
a.rot_prob_ss_non_rfs,
a.rot_prob_ww_rfs,
a.rot_prob_ww_non_rfs,
a.rot_prob_cs_rfs,
a.rot_prob_cs_non_rfs,
a.rot_prob_cw_rfs,
a.rot_prob_cw_non_rfs,

a.rot_prob_ss_rfs + a.rot_prob_ww_rfs as rot_prob_oo_rfs,
a.rot_prob_ss_non_rfs + a.rot_prob_ww_non_rfs as rot_prob_oo_non_rfs,

a.rot_prob_cs_rfs + a.rot_prob_cw_rfs as rot_prob_co_rfs,
a.rot_prob_cs_non_rfs + a.rot_prob_cw_non_rfs as rot_prob_co_non_rfs,*/



-----------  change ------------------------------------------------------------------

--The change in acres of continuous corn in each county.
sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)) as acres_change_rot_cc, 

sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)) as acres_change_rot_ss, 

sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)) as acres_change_rot_ww,

sum(a.acres * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)) as acres_change_rot_cs, 

sum(a.acres * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)) as acres_change_rot_cw, 

--sum(a.acres * (a.rot_prob_oo_rfs - a.rot_prob_oo_non_rfs)) as acres_change_rot_oo,
sum(a.acres * ((a.rot_prob_ss_rfs + a.rot_prob_ww_rfs) - (a.rot_prob_ss_non_rfs + a.rot_prob_ww_non_rfs))) as acres_change_rot_oo,

--sum(a.acres * (a.rot_prob_co_rfs - a.rot_prob_co_non_rfs)) as acres_change_rot_co, 
sum(a.acres * ((a.rot_prob_cs_rfs + a.rot_prob_cw_rfs) - (a.rot_prob_cs_non_rfs + a.rot_prob_cw_non_rfs))) as acres_change_rot_co, 


---------------change awa -------------------------------------------------------------

--change of probabilities of continuous corn due to RFS  (county-level area-weighted average)
((sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)))/ SUM(a.acres)*100) as acres_change_rot_cc_awa, 

((sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs))))/ SUM(a.acres)*100 as acres_change_rot_ss_awa, 

((sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs))))/ SUM(a.acres)*100 as acres_change_rot_ww_awa,

((sum(a.acres * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs))))/ SUM(a.acres)*100 as acres_change_rot_cs_awa, 

((sum(a.acres * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs))))/ SUM(a.acres)*100 as acres_change_rot_cw_awa, 

--((sum(a.acres * (a.rot_prob_oo_rfs - a.rot_prob_oo_non_rfs))))/ SUM(a.acres)*100 as acres_change_rot_oo_awa,
((sum(a.acres * ((a.rot_prob_ss_rfs + a.rot_prob_ww_rfs) - (a.rot_prob_ss_non_rfs + a.rot_prob_ww_non_rfs)))))/ SUM(a.acres)*100 as acres_change_rot_oo_awa,  

--((sum(a.acres * (a.rot_prob_co_rfs - a.rot_prob_co_non_rfs))))/ SUM(a.acres)*100 as acres_change_rot_co_awa,
((sum(a.acres * ((a.rot_prob_cs_rfs + a.rot_prob_cw_rfs) - (a.rot_prob_cs_non_rfs + a.rot_prob_cw_non_rfs)))))/ SUM(a.acres)*100 as acres_change_rot_co_awa,



------------- water quality ----------------------------------------------------------------------
--Increased xxxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.nleach_cc)) as acres_change_rot_cc_nleach,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.pyield_cc)) as acres_change_rot_cc_pyield,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sedyield_cc)) as acres_change_rot_cc_sedyield,


--Increased xxxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.nleach_ss)) as acres_change_rot_ss_nleach,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.pyield_ss)) as acres_change_rot_ss_pyield,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.sedyield_ss)) as acres_change_rot_ss_sedyield,


--Increased xxxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.nleach_ww)) as acres_change_rot_ww_nleach,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.pyield_ww)) as acres_change_rot_ww_pyield,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.sedyield_ww)) as acres_change_rot_ww_sedyield,


--Increased xxxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.nleach_cs)) as acres_change_rot_cs_nleach,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.pyield_cs)) as acres_change_rot_cs_pyield,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.sedyield_cs)) as acres_change_rot_cs_sedyield,


--Increased xxxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.nleach_cw)) as acres_change_rot_cw_nleach,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.pyield_cw)) as acres_change_rot_cw_pyield,
--Increased xxxxxxx from increased continuous corn in a county.
(sum(a.acres * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.sedyield_cw)) as acres_change_rot_cw_sedyield,

----skip these 15 maps above !!!!

----[nleach]
--acres_change_TOTAL_nleach = acres_change_rot_CC_nleach + acres_change_rot_SS_nleach + acres_change_rot_WW_nleach + acres_change_rot_CS_nleach + acres_change_rot_CW_nleach
((sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.nleach_cc))) + ((sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.nleach_ss))) + ((sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.nleach_ww))) +
((sum(a.acres * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.nleach_cs))) + ((sum(a.acres * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.nleach_cw))) as acres_change_total_nleach,

----[pyield]
((sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.pyield_cc))) + ((sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.pyield_ss))) + ((sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.pyield_ww))) +
((sum(a.acres * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.pyield_cs))) + ((sum(a.acres * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.pyield_cw))) as acres_change_total_pyield,

----[sedyield]
((sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sedyield_cc))) + ((sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.sedyield_ss))) + ((sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.sedyield_ww))) +
((sum(a.acres * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.sedyield_cs))) + ((sum(a.acres * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.sedyield_cw))) as acres_change_total_sedyield,

a.geom

--NOTE: the rfs_intesification dataset is the one with the bad objectid so need to link to this
FROM synthesis_intensification.rfs_intensification_agroibis_counties as a 


GROUP BY
  a.atlas_stco,
  a.acres_calc,
  a.geom

