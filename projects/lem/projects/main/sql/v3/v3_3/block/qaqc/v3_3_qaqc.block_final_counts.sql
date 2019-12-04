-----Description: create table that contains the records with majority 11 (water) and label these records null using the look up table

------this is the majority of the counts 
CREATE TABLE v3_3_qaqc.block_final_counts as 
SELECT
'query1' as query, 
count(geoid)
FROM 
  v3_3_main.block_main 
WHERE block_main.nwalt NOT IN (11,21,60) AND block_main.nwalt_rc IS NOT NULL

UNION
------ null all nwalt60 and nwalt11 records (note: might need to rerun script with different where clause condtion)
SELECT
'query2' as query, 
count(geoid)
FROM 
  v3_3_main.block_main 
--WHERE block_main.nwalt IN (11,60) OR nwalt IS NULL
WHERE block_main.nwalt IN (11,60)


UNION

-------all the nwalt 21 records that dont have to be interpoloated
SELECT
'query3' as query, 
count(geoid)
FROM 
  v3_3_main.block_main 
WHERE block_main.nwalt = 21 AND block_main.nwalt_rc IS NOT NULL



------records that are interpolated from neighbors row 34,18
UNION
SELECT
'query4' as query, 
count(geoid)
FROM v3_3_refine.block_main_nwalt_null_neighbor_values_mode_formatted




