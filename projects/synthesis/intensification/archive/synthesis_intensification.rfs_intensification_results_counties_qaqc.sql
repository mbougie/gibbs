----[sedyield]
SELECT
((sum((a.acres*0.00404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sedyield_cc))) + ((sum((a.acres*0.00404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.sedyield_ss))) + ((sum((a.acres*0.00404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.sedyield_ww))) +
((sum((a.acres*0.00404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.sedyield_cs))) + ((sum((a.acres*0.00404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.sedyield_cw))) as hectares_change_total_sedyield
FROM 
  synthesis_intensification.rfs_intensification_agroibis_counties as a
WHERE atlas_stco = '01003'

--Hectares  117.41



SELECT
objectid,
a.acres*0.00404686 as hectares,
a.rot_prob_cc_rfs,
a.rot_prob_cc_non_rfs,
(a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs) net_prob,
a.sedyield_cc, ---sediment yield conversion impact [tons] per hectare
a.sedyield_cc * (a.acres*0.00404686) as total_sed,
(a.sedyield_cc * (a.acres*0.00404686))*(a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs) as sed_due_to_rfs
--(a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sedyield_cc as sed_rfs,  ---sediment due to the rfs 
--(a.acres*0.00404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs), ---acres_change_rot_cc
--(a.acres*0.00404686) * ((a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sedyield_cc) as hectares_change_rot_cc_sedyield
FROM 
  synthesis_intensification.rfs_intensification_agroibis_counties as a
--WHERE atlas_stco = '01003'
WHERE atlas_stco = '01003' 



SELECT
atlas_stco,
sum(a.sedyield_cc * (a.acres*0.00404686)) as total_sed,


------get the sediment erosion of each patch and then sum up by county
((sum((a.acres*0.00404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sedyield_cc))) as cc,
((sum((a.acres*0.00404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.sedyield_ss))) as ss,
((sum((a.acres*0.00404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.sedyield_ww))) as ww,
((sum((a.acres*0.00404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.sedyield_cs))) as cs,
((sum((a.acres*0.00404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.sedyield_cw))) as cw,

----[sedyield      note:unites in km2]
((sum((a.acres*0.00404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sedyield_cc))) + ((sum((a.acres*0.00404686) * (a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.sedyield_ss))) + ((sum((a.acres*0.00404686) * (a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.sedyield_ww))) +
((sum((a.acres*0.00404686) * (a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.sedyield_cs))) + ((sum((a.acres*0.00404686) * (a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.sedyield_cw))) as hectares_change_total_sedyield

--(a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sedyield_cc as sed_rfs,  ---sediment due to the rfs 
--(a.acres*0.00404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs), ---acres_change_rot_cc
--(a.acres*0.00404686) * ((a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sedyield_cc) as hectares_change_rot_cc_sedyield
FROM 
  synthesis_intensification.rfs_intensification_agroibis_counties as a
--WHERE atlas_stco = '01003' 
group by atlas_stco
order by atlas_stco 



SELECT
sum((a.acres*0.00404686)),
((sum((a.acres*0.00404686) * (a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sedyield_cc)))
FROM 
  synthesis_intensification.rfs_intensification_agroibis_counties as a
WHERE objectid=3566801 OR objectid=3566814





---  16.40
SELECT
sum((a.acres*0.00404686)),
sum((a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sedyield_cc),
sum((a.rot_prob_cc_rfs - a.rot_prob_cc_non_rfs)*a.sedyield_cc*.004046886)
FROM 
  synthesis_intensification.rfs_intensification_agroibis_counties as a
WHERE atlas_stco = '01003'


117.41 * 16.40 = 1925


---  55.78
SELECT
sum((a.rot_prob_ss_rfs - a.rot_prob_ss_non_rfs)*a.sedyield_ss)
FROM 
  synthesis_intensification.rfs_intensification_agroibis_counties as a
WHERE atlas_stco = '01003'

117.41 * 55.78 = 6549



---  1.09
SELECT
sum((a.rot_prob_ww_rfs - a.rot_prob_ww_non_rfs)*a.sedyield_ww)
FROM 
  synthesis_intensification.rfs_intensification_agroibis_counties as a
WHERE atlas_stco = '01003'

117.41 * 1.09 = 128

---  -86.03
SELECT
sum((a.rot_prob_cs_rfs - a.rot_prob_cs_non_rfs)*a.sedyield_cs)
FROM 
  synthesis_intensification.rfs_intensification_agroibis_counties as a
WHERE atlas_stco = '01003'

117.41 * -86.03 = -10,101


---  -1.79
SELECT
sum((a.rot_prob_cw_rfs - a.rot_prob_cw_non_rfs)*a.sedyield_cw)
FROM 
  synthesis_intensification.rfs_intensification_agroibis_counties as a
WHERE atlas_stco = '01003'

117.41 * -1.79 = -210

