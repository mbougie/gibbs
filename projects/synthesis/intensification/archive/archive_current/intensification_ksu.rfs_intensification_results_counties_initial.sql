create table intensification_ksu.rfs_intensification_results_counties as 

SELECT 
--count(a.objectid) as objectid_count,
a.atlas_stco,

--------------------------------------------------------------------------------------
-----------  areas ------------------------------------------------------------------
--------------------------------------------------------------------------------------
sum(a.acres) as acres,
sum(a.acres*0.404686) as hectares,
sum(a.acres*0.00404686) as km2,

/*  ---dont need to run this code-----------------
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
*/



--------------------------------------------------------------------------------------------------
------------- water quality ----------------------------------------------------------------------
--------------------------------------------------------------------------------------------------


--------- CC -----------------------------------------------------------------------------------------------
----[PYIELD]       
----Note:units in kg per hectares
(sum((a.acres*0.404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.pyield_cc)) as kg_change_rot_cc_pyield,

----[NLEACH]        
----Note:units in kg per hectares
(sum((a.acres*0.404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.nleach_cc)) as kg_change_rot_cc_nleach,

----[SEDYIELD]      
----Note:units in metric tons per km2
(sum((a.acres*0.00404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sedyield_cc)) as ton_change_rot_cc_sedyield,




--------- OO -----------------------------------------------------------------------------------------------
----[PYIELD]       
----Note:units in kg per hectares
((sum((a.acres*0.404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.pyield_ss))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.pyield_ww))) as kg_change_rot_oo_pyield,


----[NLEACH]        
----Note:units in kg per hectares
((sum((a.acres*0.404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.nleach_ss))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.nleach_ww))) as kg_change_rot_oo_nleach,



----[SEDYIELD]      
----Note:units in metric tons per km2
((sum((a.acres*0.00404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.sedyield_ss))) + 
((sum((a.acres*0.00404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.sedyield_ww))) as ton_change_rot_oo_sedyield,



--------- CO -----------------------------------------------------------------------------------------------
----[PYIELD]       
----Note:units in kg per hectares
((sum((a.acres*0.404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.pyield_cs))) + 
((sum((a.acres*0.404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.pyield_cw))) as kg_change_rot_co_pyield,


----[NLEACH]        
----Note:units in kg per hectares
((sum((a.acres*0.404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.nleach_cs))) + 
((sum((a.acres*0.404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.nleach_cw))) as kg_change_rot_co_nleach,



----[SEDYIELD]      
----Note:units in metric tons per km2
((sum((a.acres*0.00404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.sedyield_cs))) + 
((sum((a.acres*0.00404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.sedyield_cw))) as ton_change_rot_co_sedyield,



--------- TOTAL -----------------------------------------------------------------------------------------------
----[PYIELD]       
----Note:units in kg per hectares
((sum((a.acres*0.404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.pyield_cc))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.pyield_ss))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.pyield_ww))) +
((sum((a.acres*0.404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.pyield_cs))) + 
((sum((a.acres*0.404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.pyield_cw))) as kg_change_total_pyield,



----[NLEACH]        
----Note:units in kg per hectares
((sum((a.acres*0.404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.nleach_cc))) +
((sum((a.acres*0.404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.nleach_ss))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.nleach_ww))) +
((sum((a.acres*0.404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.nleach_cs))) + 
((sum((a.acres*0.404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.nleach_cw))) as kg_change_total_nleach,



----[SEDYIELD]      
----Note:units in metric tons per km2
((sum((a.acres*0.00404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sedyield_cc))) + 
((sum((a.acres*0.00404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.sedyield_ss))) + 
((sum((a.acres*0.00404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.sedyield_ww))) +
((sum((a.acres*0.00404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.sedyield_cs))) + 
((sum((a.acres*0.00404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.sedyield_cw))) as ton_change_total_sedyield,

a.geom


FROM intensification_ksu.rfs_intensification_rasters_counties as a 


GROUP BY
  a.atlas_stco,
  a.acres_calc,
  a.geom














