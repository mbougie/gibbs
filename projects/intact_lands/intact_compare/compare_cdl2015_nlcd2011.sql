SELECT 
  intact_compare.value, 
  intact_compare.count, 
  intact_compare.intact_clu_b, 
  intact_compare.intact_nlcd, 
  intact_compare_cdl2015_nlcd2011.value, 
  intact_compare_cdl2015_nlcd2011.count, 
  intact_compare_cdl2015_nlcd2011.intact_compare, 
  intact_compare_cdl2015_nlcd2011.cdl30_2015, 
  intact_compare_cdl2015_nlcd2011.nlcd30_2011
FROM 
  intact_compare.intact_compare, 
  intact_compare.intact_compare_cdl2015_nlcd2011
WHERE 
  intact_compare.value = intact_compare_cdl2015_nlcd2011.intact_compare AND intact_compare.value = 2
order by intact_compare_cdl2015_nlcd2011.count desc