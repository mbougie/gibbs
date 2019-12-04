---- Query returned successfully: 813096 rows affected, 717766 ms execution time.

create table v3_3_refine.block_main_nwalt_null as 

SELECT 
  * 
FROM 
  v3_3_main.block_main
WHERE nwalt IS NULL

order by hectares desc



/*
dataset to show all null values for nwalt and nwalt_rc as well

reason for these being null:
1. no raster center inside of polygon (these are very small or very linear (corridor)polygons)
2. larger polygons with no raster under it (i.e. islands)
3. for nwalt_rc some data



things to do:



what have I learned:
1. need to go back and fill small poygons (i.e. refinement)  --- for sure!!
2. need to null out polygons that have been reclassed?  --- TBD



Questions:
Do I need to refine nwalt 21 records or can I just reference the nwalt_rc column?




*/