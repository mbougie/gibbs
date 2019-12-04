SELECT 
  intact2008.value,
  intact2008.count as intact2008_count,
  intact2008.count*0.222395 as acres,
    converted.value,
  converted.count as converted_count,
  converted.count *0.222395 as acres,
   intact2017.value,
  intact2017.count as intact2017_count,
  intact2017.count *0.222395 as acres
 --- intact2008.count - converted.count as remain_intact
FROM 
  intactland_converted.intactlands_s35_converted as converted 
FULL OUTER JOIN
  intactland_converted.intactlands_cdl2008 as intact2008 
USING(value)
FULL OUTER JOIN
  intactland_converted.intactlands_cdl2017 as intact2017
USING(value)
ORDER BY intact2017.value,intact2008.value,converted.value