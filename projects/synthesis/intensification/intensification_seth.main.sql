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


--------------------------------------------------------------------------------------------------
------------- water quality ----------------------------------------------------------------------
--------------------------------------------------------------------------------------------------


--------- CC -----------------------------------------------------------------------------------------------
----[PYIELD]       
----Note:units in kg per hectares
(sum((a.acres*0.404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.phos_cc)) as kg_change_rot_cc_pyield,

----[NLEACH]        
----Note:units in kg per hectares
(sum((a.acres*0.404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.nlch_cc)) as kg_change_rot_cc_nleach,

----[SEDYIELD]      
----Note:units in metric tons per km2
(sum((a.acres*0.00404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sed_cc)) as ton_change_rot_cc_sedyield,




--------- OO -----------------------------------------------------------------------------------------------
----[PYIELD]       
----Note:units in kg per hectares
((sum((a.acres*0.404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.phos_ss))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.phos_ww))) as kg_change_rot_oo_pyield,


----[NLEACH]        
----Note:units in kg per hectares
((sum((a.acres*0.404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.nlch_ss))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.nlch_ww))) as kg_change_rot_oo_nleach,



----[SEDYIELD]      
----Note:units in metric tons per km2
((sum((a.acres*0.00404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.sed_ss))) + 
((sum((a.acres*0.00404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.sed_ww))) as ton_change_rot_oo_sedyield,



--------- CO -----------------------------------------------------------------------------------------------
----[PYIELD]       
----Note:units in kg per hectares
((sum((a.acres*0.404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.phos_cs))) + 
((sum((a.acres*0.404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.phos_cw))) as kg_change_rot_co_pyield,


----[NLEACH]        
----Note:units in kg per hectares
((sum((a.acres*0.404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.nlch_cs))) + 
((sum((a.acres*0.404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.nlch_cw))) as kg_change_rot_co_nleach,



----[SEDYIELD]      
----Note:units in metric tons per km2
((sum((a.acres*0.00404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.sed_cs))) + 
((sum((a.acres*0.00404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.sed_cw))) as ton_change_rot_co_sedyield,



--------- TOTAL -----------------------------------------------------------------------------------------------
----[PYIELD]       
----Note:units in kg per hectares
((sum((a.acres*0.404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.phos_cc))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.phos_ss))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.phos_ww))) +
((sum((a.acres*0.404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.phos_cs))) + 
((sum((a.acres*0.404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.phos_cw))) as kg_change_total_pyield,



----[NLEACH]        
----Note:units in kg per hectares
((sum((a.acres*0.404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.nlch_cc))) +
((sum((a.acres*0.404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.nlch_ss))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.nlch_ww))) +
((sum((a.acres*0.404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.nlch_cs))) + 
((sum((a.acres*0.404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.nlch_cw))) as kg_change_total_nleach,



----[SEDYIELD]      
----Note:units in metric tons per km2
((sum((a.acres*0.00404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sed_cc))) + 
((sum((a.acres*0.00404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.sed_ss))) + 
((sum((a.acres*0.00404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.sed_ww))) +
((sum((a.acres*0.00404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.sed_cs))) + 
((sum((a.acres*0.00404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.sed_cw))) as ton_change_total_sedyield,
a.geom

FROM 

( SELECT
main.unique_id, 
main.mlra, 
main.lon, 
main.lat, 
main.acres, 
main.rot_prob_cc_rfs, 
main.rot_prob_ss_rfs, 
main.rot_prob_ww_rfs, 
main.rot_prob_sc_rfs as rot_prob_cs_rfs, 
main.rot_prob_wc_rfs as rot_prob_cw_rfs, 
main.rot_prob_cc_non_rfs, 
main.rot_prob_ss_non_rfs, 
main.rot_prob_ww_non_rfs, 
main.rot_prob_sc_non_rfs as rot_prob_cs_non_rfs, 
main.rot_prob_wc_non_rfs as rot_prob_cw_non_rfs, 

intensification_seth.cc_nlch_fips_summary as nlch_cc,
intensification_seth.ss_nlch_fips_summary as nlch_ss,
intensification_seth.cs_nlch_fips_summary as nlch_cs,
intensification_seth.ww_nlch_fips_summary as nlch_ww,
intensification_seth.cw_nlch_fips_summary as nlch_cw,

intensification_seth.cc_phos_fips_summary as phos_cc,
intensification_seth.ss_phos_fips_summary as phos_ss,
intensification_seth.cs_phos_fips_summary as phos_cs,
intensification_seth.ww_phos_fips_summary as phos_ww,
intensification_seth.cw_phos_fips_summary as phos_cw,

intensification_seth.cc_sed_fips_summary as sed_cc,
intensification_seth.ss_sed_fips_summary as sed_ss,
intensification_seth.cs_sed_fips_summary as sed_cs,
intensification_seth.ww_sed_fips_summary as sed_ww,
intensification_seth.cw_sed_fips_summary as sed_cw,

intensification_seth.cc_n2o_fips_summary as n2o_cc,
intensification_seth.ss_n2o_fips_summary as n2o_ss,
intensification_seth.cs_n2o_fips_summary as n2o_cs,
intensification_seth.ww_n2o_fips_summary as n2o_ww,
intensification_seth.cw_n2o_fips_summary as n2o_cw,

intensification_seth.cc_napp_fips_summary as napp_cc,
intensification_seth.ss_napp_fips_summary as napp_ss,
intensification_seth.cs_napp_fips_summary as napp_cs,
intensification_seth.ww_napp_fips_summary as napp_ww,
intensification_seth.cw_napp_fips_summary as napp_cw,

counties.atlas_st, 
counties.st_abbrev,
counties.atlas_stco, 
counties.state_name, 
counties.acres_calc,
counties.geom 
FROM 
intensification_ksu.rfs_intensification as main INNER JOIN  USING(unique_id),
spatial.counties
WHERE 
st_transform(main.geom,5070) && counties.geom AND
ST_Within(st_transform(main.geom,5070), counties.geom)) as a 


GROUP BY
  a.atlas_stco,
  a.acres_calc,
  a.geom











