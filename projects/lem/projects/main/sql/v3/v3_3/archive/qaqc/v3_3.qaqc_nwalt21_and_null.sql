create table v3_3.qaqc_nwalt21_and_null as
-----Query returned successfully: 1519772 rows affected, 667752 ms execution time.
SELECT 
  * 
FROM 
  v3_3.block_main
WHERE 
(nwalt IN (21) OR nwalt IS NULL)



