-----Query returned successfully: 1,017,011 rows affected, 1183142 ms execution time.

create table v3_3_qaqc.tract_main_nwalt_11_21_60_null as 

SELECT 
  * 
FROM 
  v3_3_main.tract_main
WHERE nwalt in (11,21,60) OR nwalt IS NULL

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
Why is 


---*************************************************************************************************************
NOTE: these are all the nwalt records that had values 11,21,60 with ALL nwalt values (null AND integer)
---*****************************************************************************************************


*/