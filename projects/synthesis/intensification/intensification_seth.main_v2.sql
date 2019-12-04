create table intensification.rfs_intensification_results_counties as 

SELECT 
--count(a.objectid) as objectid_count,
a.atlas_stco,
a.fips,
sum(a.m2) as m2,
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
((sum((a.acres*0.404686) * (a.rot_prob_sc_rfs - a.rot_prob_sc_non_rfs)*a.phos_sc))) + 
((sum((a.acres*0.404686) * (a.rot_prob_wc_rfs - a.rot_prob_wc_non_rfs)*a.phos_wc))) as kg_change_rot_co_pyield,


----[NLEACH]        
----Note:units in kg per hectares
((sum((a.acres*0.404686) * (a.rot_prob_sc_rfs - a.rot_prob_sc_non_rfs)*a.nlch_sc))) + 
((sum((a.acres*0.404686) * (a.rot_prob_wc_rfs - a.rot_prob_wc_non_rfs)*a.nlch_wc))) as kg_change_rot_co_nleach,



----[SEDYIELD]      
----Note:units in metric tons per km2
((sum((a.acres*0.00404686) * (a.rot_prob_sc_rfs - a.rot_prob_sc_non_rfs)*a.sed_sc))) + 
((sum((a.acres*0.00404686) * (a.rot_prob_wc_rfs - a.rot_prob_wc_non_rfs)*a.sed_wc))) as ton_change_rot_co_sedyield,



--------- TOTAL -----------------------------------------------------------------------------------------------
----[PYIELD]       
----Note:units in kg per hectares
((sum((a.acres*0.404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.phos_cc))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.phos_ss))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.phos_ww))) +
((sum((a.acres*0.404686) * (a.rot_prob_sc_rfs - a.rot_prob_sc_non_rfs)*a.phos_sc))) + 
((sum((a.acres*0.404686) * (a.rot_prob_wc_rfs - a.rot_prob_wc_non_rfs)*a.phos_wc))) as kg_change_total_pyield,



----[NLEACH]        
----Note:units in kg per hectares
((sum((a.acres*0.404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.nlch_cc))) +
((sum((a.acres*0.404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.nlch_ss))) + 
((sum((a.acres*0.404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.nlch_ww))) +
((sum((a.acres*0.404686) * (a.rot_prob_sc_rfs - a.rot_prob_sc_non_rfs)*a.nlch_sc))) + 
((sum((a.acres*0.404686) * (a.rot_prob_wc_rfs - a.rot_prob_wc_non_rfs)*a.nlch_wc))) as kg_change_total_nleach,



----[SEDYIELD]      
----Note:units in metric tons per km2
((sum((a.acres*0.00404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sed_cc))) + 
((sum((a.acres*0.00404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.sed_ss))) + 
((sum((a.acres*0.00404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.sed_ww))) +
((sum((a.acres*0.00404686) * (a.rot_prob_sc_rfs - a.rot_prob_sc_non_rfs)*a.sed_sc))) + 
((sum((a.acres*0.00404686) * (a.rot_prob_wc_rfs - a.rot_prob_wc_non_rfs)*a.sed_wc))) as ton_change_total_sedyield,
a.geom

FROM 

(SELECT
main.unique_id, 
main.mlra, 
main.lon, 
main.lat, 
main.acres, 
main.rot_prob_cc_rfs, 
main.rot_prob_ss_rfs, 
main.rot_prob_ww_rfs, 
main.rot_prob_sc_rfs,
main.rot_prob_wc_rfs,
main.rot_prob_cc_non_rfs, 
main.rot_prob_ss_non_rfs, 
main.rot_prob_ww_non_rfs, 
main.rot_prob_sc_non_rfs,
main.rot_prob_wc_non_rfs, 

nlch_cc.mean as nlch_cc,
nlch_ss.mean as nlch_ss,
nlch_sc.mean as nlch_sc,
nlch_ww.mean as nlch_ww,
nlch_wc.mean as nlch_wc,

phos_cc.mean as phos_cc,
phos_ss.mean as phos_ss,
phos_sc.mean as phos_sc,
phos_ww.mean as phos_ww,
phos_wc.mean as phos_wc,

sed_cc.mean as sed_cc,
sed_ss.mean as sed_ss,
sed_sc.mean as sed_sc,
sed_ww.mean as sed_ww,
sed_wc.mean as sed_wc,

n2o_cc.mean as n2o_cc,
n2o_ss.mean as n2o_ss,
n2o_sc.mean as n2o_sc,
n2o_ww.mean as n2o_ww,
n2o_wc.mean as n2o_wc,

napp_cc.mean as napp_cc,
napp_ss.mean as napp_ss,
napp_sc.mean as napp_sc,
napp_ww.mean as napp_ww,
napp_wc.mean as napp_wc,

main.atlas_st, 
main.st_abbrev,
main.atlas_stco,
main.fips,
main.m2, 
main.state_name, 
main.acres_calc,
main.geom 
FROM 
intensification.rfs_intensification_w_counties as main INNER JOIN 
intensification_seth.cc_nlch_fips_summary as nlch_cc USING(fips) INNER JOIN 
intensification_seth.ss_nlch_fips_summary as nlch_ss USING(fips) INNER JOIN 
intensification_seth.sc_nlch_fips_summary as nlch_sc USING(fips) INNER JOIN 
intensification_seth.ww_nlch_fips_summary as nlch_ww USING(fips) INNER JOIN 
intensification_seth.wc_nlch_fips_summary as nlch_wc USING(fips) INNER JOIN 

intensification_seth.cc_phos_fips_summary as phos_cc USING(fips) INNER JOIN 
intensification_seth.ss_phos_fips_summary as phos_ss USING(fips) INNER JOIN 
intensification_seth.sc_phos_fips_summary as phos_sc USING(fips) INNER JOIN 
intensification_seth.ww_phos_fips_summary as phos_ww USING(fips) INNER JOIN 
intensification_seth.wc_phos_fips_summary as phos_wc USING(fips) INNER JOIN 

intensification_seth.cc_sed_fips_summary as sed_cc USING(fips) INNER JOIN 
intensification_seth.ss_sed_fips_summary as sed_ss USING(fips) INNER JOIN 
intensification_seth.sc_sed_fips_summary as sed_sc USING(fips) INNER JOIN 
intensification_seth.ww_sed_fips_summary as sed_ww USING(fips) INNER JOIN 
intensification_seth.wc_sed_fips_summary as sed_wc USING(fips) INNER JOIN 

intensification_seth.cc_n2o_fips_summary as n2o_cc USING(fips) INNER JOIN 
intensification_seth.ss_n2o_fips_summary as n2o_ss USING(fips) INNER JOIN 
intensification_seth.sc_n2o_fips_summary as n2o_sc USING(fips) INNER JOIN 
intensification_seth.ww_n2o_fips_summary as n2o_ww USING(fips) INNER JOIN 
intensification_seth.wc_n2o_fips_summary as n2o_wc USING(fips) INNER JOIN 

intensification_seth.cc_napp_fips_summary as napp_cc USING(fips) INNER JOIN 
intensification_seth.ss_napp_fips_summary as napp_ss USING(fips) INNER JOIN 
intensification_seth.sc_napp_fips_summary as napp_sc USING(fips) INNER JOIN 
intensification_seth.ww_napp_fips_summary as napp_ww USING(fips) INNER JOIN 
intensification_seth.wc_napp_fips_summary as napp_wc USING(fips)) as a


GROUP BY
a.atlas_stco,
a.fips,
a.acres_calc,
a.geom 











