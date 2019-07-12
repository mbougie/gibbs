--create table synthesis_intensification.rfs_intensification_results_national_t2 as 

SELECT 

--------------------------------------------------------------------------------------
-----------  change ------------------------------------------------------------------
--------------------------------------------------------------------------------------
--The change in acres of continuous corn in each county.

--atlas_stco,

--------- CC -----------------------------------------------------------------------------------------------
(sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)))/sum(a.acres)*100 as acres_change_rot_cc_awa

--------- SS -----------------------------------------------------------------------------------------------
/* sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)) as acres_change_rot_ss, 

--------- WW -----------------------------------------------------------------------------------------------
sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)) as acres_change_rot_ww,

--------- CS -----------------------------------------------------------------------------------------------
sum(a.acres * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)) as acres_change_rot_cs, 

--------- CW -----------------------------------------------------------------------------------------------
sum(a.acres * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)) as acres_change_rot_cw, 

--------- OO -----------------------------------------------------------------------------------------------
sum(a.acres * ((a.rot_prob_ss_rfs + a.rot_prob_ww_rfs) - (a.rot_prob_ss_non_rfs + a.rot_prob_ww_non_rfs))) as acres_change_rot_oo,

--------- CO -----------------------------------------------------------------------------------------------
sum(a.acres * ((a.rot_prob_cs_rfs + a.rot_prob_cw_rfs) - (a.rot_prob_cs_non_rfs + a.rot_prob_cw_non_rfs))) as acres_change_rot_co, */



FROM synthesis_intensification.rfs_intensification_agroibis_counties as a 


where atlas_stco = '01001'

--WHERE atlas_stco = '01001'