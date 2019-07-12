
---add binary column
ALTER TABLE intact_nlcd.nlcd_combine ADD COLUMN b integer;

---update binary column to zero as default
--- 26097 rows affected
update intact_nlcd.nlcd_combine SET b = 101;



---update binary column to 100 where records have had crop in them
---11863 rows affected with 81 nad 82 considered crop
---6744 rows affected with only 82 considered crop
update intact_nlcd.nlcd_combine SET b = 100 WHERE value IN (
SELECT 
 -- nlcd_combine.index, 
  --nlcd_combine.objectid, 
  nlcd_combine.value 
 /*nlcd_combine.count, 
  nlcd_combine.nlcd30_1992, 
  nlcd_combine.nlcd30_2001, 
  nlcd_combine.nlcd30_2006, 
  nlcd_combine.nlcd30_2011,
  nlcd_combine.b*/
FROM 
  intact_nlcd.nlcd_combine
WHERE 
  ----qaqc there are 16 records here i.e. 2 to the 4th power
    /*nlcd_combine.nlcd30_1992 in (81,82) AND
  nlcd_combine.nlcd30_2001 in (81,82) AND
  nlcd_combine.nlcd30_2006 in (81,82) AND
  nlcd_combine.nlcd30_2011 in (81,82)*/

  ---- QAQC: fill this in
  nlcd_combine.nlcd30_1992 in (81,82) OR
  nlcd_combine.nlcd30_2001 in (81,82) OR
  nlcd_combine.nlcd30_2006 in (81,82) OR
  nlcd_combine.nlcd30_2011 in (81,82)



  ---- QAQC: fill this in
  /*nlcd_combine.nlcd30_1992 in (82) OR
  nlcd_combine.nlcd30_2001 in (82) OR
  nlcd_combine.nlcd30_2006 in (82) OR
  nlcd_combine.nlcd30_2011 in (82)*/

)


---update binary column to 0 where records have 0(i.e. no data) in them
---the 1992 nlcd data is different than the others in this regard 
---129 records effected
update intact_nlcd.nlcd_combine SET b = 0 WHERE value IN (
SELECT 
 -- nlcd_combine.index, 
  --nlcd_combine.objectid, 
  nlcd_combine.value 
 /*nlcd_combine.count, 
  nlcd_combine.nlcd30_1992, 
  nlcd_combine.nlcd30_2001, 
  nlcd_combine.nlcd30_2006, 
  nlcd_combine.nlcd30_2011,
  nlcd_combine.b*/
FROM 
  intact_nlcd.nlcd_combine
WHERE 
  ---- QAQC: fill this in
  nlcd_combine.nlcd30_1992 in (0) OR
  nlcd_combine.nlcd30_2001 in (0) OR
  nlcd_combine.nlcd30_2006 in (0) OR
  nlcd_combine.nlcd30_2011 in (0)

)





  

