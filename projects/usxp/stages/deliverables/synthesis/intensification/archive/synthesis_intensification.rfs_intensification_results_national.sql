create table synthesis_intensification.rfs_intensification_results_counties as 

SELECT 
count(a.objectid) as objectid_count,
a.atlas_stco,

--------------------------------------------------------------------------------------
-----------  areas ------------------------------------------------------------------
--------------------------------------------------------------------------------------
sum(a.acres) as acres,
sum(a.acres*0.404686) as hectares,
sum(a.acres*0.00404686) as km2,


--------------------------------------------------------------------------------------
-----------  change ------------------------------------------------------------------
--------------------------------------------------------------------------------------
--The change in acres of continuous corn in each county.

--------- CC -----------------------------------------------------------------------------------------------
sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)) as acres_change_rot_cc, 

--------- SS -----------------------------------------------------------------------------------------------
sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)) as acres_change_rot_ss, 

--------- WW -----------------------------------------------------------------------------------------------
sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)) as acres_change_rot_ww,

--------- CS -----------------------------------------------------------------------------------------------
sum(a.acres * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)) as acres_change_rot_cs, 

--------- CW -----------------------------------------------------------------------------------------------
sum(a.acres * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)) as acres_change_rot_cw, 

--------- OO -----------------------------------------------------------------------------------------------
sum(a.acres * ((a.rot_prob_ss_rfs + a.rot_prob_ww_rfs) - (a.rot_prob_ss_non_rfs + a.rot_prob_ww_non_rfs))) as acres_change_rot_oo,

--------- CO -----------------------------------------------------------------------------------------------
sum(a.acres * ((a.rot_prob_cs_rfs + a.rot_prob_cw_rfs) - (a.rot_prob_cs_non_rfs + a.rot_prob_cw_non_rfs))) as acres_change_rot_co, 



---------------------------------------------------------------------------------------
---------------change awa -------------------------------------------------------------
---------------------------------------------------------------------------------------
--change of probabilities of xx due to RFS  (county-level area-weighted average)

--------- CC -----------------------------------------------------------------------------------------------
((sum(a.acres * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)))/ SUM(a.acres)*100) as acres_change_rot_cc_awa, 

--------- SS -----------------------------------------------------------------------------------------------
((sum(a.acres * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs))))/ SUM(a.acres)*100 as acres_change_rot_ss_awa, 

--------- WW -----------------------------------------------------------------------------------------------
((sum(a.acres * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs))))/ SUM(a.acres)*100 as acres_change_rot_ww_awa,

--------- CS -----------------------------------------------------------------------------------------------
((sum(a.acres * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs))))/ SUM(a.acres)*100 as acres_change_rot_cs_awa, 

--------- CW -----------------------------------------------------------------------------------------------
((sum(a.acres * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs))))/ SUM(a.acres)*100 as acres_change_rot_cw_awa, 

--------- OO -----------------------------------------------------------------------------------------------
((sum(a.acres * ((a.rot_prob_ss_rfs + a.rot_prob_ww_rfs) - (a.rot_prob_ss_non_rfs + a.rot_prob_ww_non_rfs)))))/ SUM(a.acres)*100 as acres_change_rot_oo_awa,  

--------- CO -----------------------------------------------------------------------------------------------
((sum(a.acres * ((a.rot_prob_cs_rfs + a.rot_prob_cw_rfs) - (a.rot_prob_cs_non_rfs + a.rot_prob_cw_non_rfs)))))/ SUM(a.acres)*100 as acres_change_rot_co_awa,




--------------------------------------------------------------------------------------------------
------------- N related ----------------------------------------------------------------------
--------------------------------------------------------------------------------------------------

--------- CC -----------------------------------------------------------------------------------------------
----[Napplication]        
----Note:units in hectares
(sum((a.acres*0.404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.napplication_cc)) as hectares_change_rot_cc_napp,

----[N2O]       
----Note:units in hectares
(sum((a.acres*0.404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.n2o_cc)) as hectares_change_rot_cc_n2o,



--------- OO -----------------------------------------------------------------------------------------------
----[Napplication]       
----Note:units in hectares
((sum((a.acres*0.404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.napplication_ss))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.napplication_ww))) as hectares_change_rot_oo_napp,

----[N2O]       
----Note:units in hectares
((sum((a.acres*0.404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.n2o_ss))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.n2o_ww))) as hectares_change_rot_oo_n2o,




--------- CO -----------------------------------------------------------------------------------------------
----[Napplication]       
----Note:units in hectares
((sum((a.acres*0.404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.napplication_cs))) + 
((sum((a.acres*0.404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.napplication_cw))) as hectares_change_rot_co_napp,

----[N2O]       
----Note:units in hectares
((sum((a.acres*0.404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.n2o_cs))) + 
((sum((a.acres*0.404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.n2o_cw))) as hectares_change_rot_co_n2o,



--------- TOTAL WATER QUALITY ----------------------------------------------------------------------------------------------

----[Napplication]       
----Note:units in hectares
((sum((a.acres*0.404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.napplication_cc))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.napplication_ss))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.napplication_ww))) +
((sum((a.acres*0.404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.napplication_cs))) + 
((sum((a.acres*0.404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.napplication_cw))) as hectares_change_total_napp,

----[N2O]       
----Note:units in hectares
((sum((a.acres*0.404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.n2o_cc))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.n2o_ss))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.n2o_ww))) +
((sum((a.acres*0.404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.n2o_cs))) + 
((sum((a.acres*0.404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.n2o_cw))) as hectares_change_total_n2o,

a.geom


FROM synthesis_intensification.rfs_intensification_agroibis_counties as a 


GROUP BY
  a.atlas_stco,
  a.acres_calc,
  a.geom

