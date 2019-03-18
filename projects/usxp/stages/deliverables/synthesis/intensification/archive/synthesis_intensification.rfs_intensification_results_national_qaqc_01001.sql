--create table synthesis_intensification.rfs_intensification_results_national_t2 as 

SELECT 

--------------------------------------------------------------------------------------
-----------  change ------------------------------------------------------------------
--------------------------------------------------------------------------------------
--The change in acres of continuous corn in each county.

--atlas_stco,
--acres,
--(a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs) as prob,
sum(a.acres),
--------- CC -----------------------------------------------------------------------------------------------
--a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs) as acres_change_rot_cc_awa

(sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)))/sum(a.acres)*100 as acres_change_rot_cc_awa





FROM synthesis_intensification.rfs_intensification_agroibis_counties as a 


where atlas_stco = '01001'

--WHERE atlas_stco = '01001'