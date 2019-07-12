SELECT 
  'wetland'::text as nc_group,
  sum(s35_ytc30_2008to2017_mmu5_bfc.acres) as acres
FROM 
  counts_cdl.s35_ytc30_2008to2017_mmu5_bfc
WHERE value IN (83,87,190,195)

UNION

SELECT 
  'shrubland'::text as nc_group,
  sum(s35_ytc30_2008to2017_mmu5_bfc.acres) as acres
FROM 
  counts_cdl.s35_ytc30_2008to2017_mmu5_bfc
WHERE value IN (64,65,131,152)

UNION

SELECT 
  'forest'::text as nc_group,
  sum(s35_ytc30_2008to2017_mmu5_bfc.acres) as acres
FROM 
  counts_cdl.s35_ytc30_2008to2017_mmu5_bfc
WHERE value IN (63,141,142,143)

UNION

SELECT 
  'grassland'::text as nc_group,
  sum(s35_ytc30_2008to2017_mmu5_bfc.acres) as acres
FROM 
  counts_cdl.s35_ytc30_2008to2017_mmu5_bfc
WHERE value IN (37,62,171,176,181)








--instance = {'series':'s35', 'scale':{'3km':100}, 'reclass':{'forest':[[63,1],[141,1],[142,1],[143,1]], 'wetland':[[83,1],[87,1],[190,1],[195,1]], 'grassland':[[37,1],[62,1],[171,1],[176,1],[181,1]], 'shrubland':[[64,1],[65,1],[131,1],[152,1]]} }
