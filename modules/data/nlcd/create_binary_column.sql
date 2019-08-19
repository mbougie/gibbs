----description: create a binary column and populate it for each lookup with logic that 81 and 82 are null and everyhting else with a pixel count is 1 (TRUE for noncrop).  Treat the unclassified (value=0) as a potential noncrop
----(the context of the value = 0 will determine if the permutaion from combine function allows it to be labeled intact)



-----------------------------------------------
--- nlcd.lookup_nlcd30_1992_v1_mod ----------------
-----------------------------------------------
SELECT 
  * 
FROM 
  nlcd.lookup_nlcd30_1992_v1_mod
WHERE count <> 0;

ALTER TABLE nlcd.lookup_nlcd30_1992_v1_mod ADD COLUMN b integer;

UPDATE nlcd.lookup_nlcd30_1992_v1_mod set b=1 WHERE value NOT in (0,81,82) AND count <> 0;

UPDATE nlcd.lookup_nlcd30_1992_v1_mod set b=0 WHERE value in (81,82);

UPDATE nlcd.lookup_nlcd30_1992_v1_mod set b=255 WHERE value = 0;
--QAQC to compare with raster statistics
SELECT 
  SUM(count) 
FROM 
  nlcd.lookup_nlcd30_1992_v1_mod
WHERE count IS NOT NULL AND value not in (81,82);



-----------------------------------------------
--- nlcd.lookup_nlcd_2001_land_cover_l48_20190424 ----------------
-----------------------------------------------
SELECT 
  * 
FROM 
  nlcd.lookup_nlcd_2001_land_cover_l48_20190424
WHERE count <> 0;

ALTER TABLE nlcd.lookup_nlcd_2001_land_cover_l48_20190424 ADD COLUMN b integer;

UPDATE nlcd.lookup_nlcd_2001_land_cover_l48_20190424 set b=1 WHERE value NOT in (0,81,82) AND count <> 0;

UPDATE nlcd.lookup_nlcd_2001_land_cover_l48_20190424 set b=0 WHERE value in (81,82);

UPDATE nlcd.lookup_nlcd_2001_land_cover_l48_20190424 set b=255 WHERE value = 0;
--QAQC to compare with raster statistics
SELECT 
  SUM(count) 
FROM 
  nlcd.lookup_nlcd_2001_land_cover_l48_20190424
WHERE count IS NOT NULL AND value not in (81,82);




-----------------------------------------------
--- nlcd.lookup_nlcd_2004_land_cover_l48_20190424 ----------------
-----------------------------------------------
SELECT 
  * 
FROM 
  nlcd.lookup_nlcd_2004_land_cover_l48_20190424
WHERE count <> 0;

ALTER TABLE nlcd.lookup_nlcd_2004_land_cover_l48_20190424 ADD COLUMN b integer;

UPDATE nlcd.lookup_nlcd_2004_land_cover_l48_20190424 set b=1 WHERE value NOT in (0,81,82) AND count <> 0;

UPDATE nlcd.lookup_nlcd_2004_land_cover_l48_20190424 set b=0 WHERE value in (81,82);

UPDATE nlcd.lookup_nlcd_2004_land_cover_l48_20190424 set b=255 WHERE value = 0;


--QAQC to compare with raster statistics
SELECT 
  SUM(count) 
FROM 
  nlcd.lookup_nlcd_2004_land_cover_l48_20190424
WHERE count IS NOT NULL AND value not in (81,82);








-----------------------------------------------
--- nlcd.lookup_nlcd_2006_land_cover_l48_20190424 ----------------
-----------------------------------------------
SELECT 
  * 
FROM 
  nlcd.lookup_nlcd_2006_land_cover_l48_20190424
WHERE count <> 0;

ALTER TABLE nlcd.lookup_nlcd_2006_land_cover_l48_20190424 ADD COLUMN b integer;

UPDATE nlcd.lookup_nlcd_2006_land_cover_l48_20190424 set b=1 WHERE value NOT in (0,81,82) AND count <> 0;

UPDATE nlcd.lookup_nlcd_2006_land_cover_l48_20190424 set b=0 WHERE value in (81,82);

UPDATE nlcd.lookup_nlcd_2006_land_cover_l48_20190424 set b=255 WHERE value = 0;


--QAQC to compare with raster statistics
SELECT 
  SUM(count) 
FROM 
  nlcd.lookup_nlcd_2006_land_cover_l48_20190424
WHERE count IS NOT NULL AND value not in (81,82);







--------------------------------------------
--- nlcd.lookup_nlcd_2008_land_cover_l48_20190424 ----------------
--------------------------------------------
SELECT 
  * 
FROM 
  nlcd.lookup_nlcd_2008_land_cover_l48_20190424
WHERE count <> 0;

ALTER TABLE nlcd.lookup_nlcd_2008_land_cover_l48_20190424 ADD COLUMN b integer;

UPDATE nlcd.lookup_nlcd_2008_land_cover_l48_20190424 set b=1 WHERE value NOT in (0,81,82) AND count <> 0;

UPDATE nlcd.lookup_nlcd_2008_land_cover_l48_20190424 set b=0 WHERE value in (81,82);

UPDATE nlcd.lookup_nlcd_2008_land_cover_l48_20190424 set b=255 WHERE value = 0;


--QAQC to compare with raster statistics
SELECT 
  SUM(count) 
FROM 
  nlcd.lookup_nlcd_2008_land_cover_l48_20190424
WHERE count IS NOT NULL AND value not in (81,82);



-----------------------------------------------
--- nlcd.lookup_nlcd_2011_land_cover_l48_20190424 ----------------
-----------------------------------------------
SELECT 
  * 
FROM 
  nlcd.lookup_nlcd_2011_land_cover_l48_20190424
WHERE count <> 0;

ALTER TABLE nlcd.lookup_nlcd_2011_land_cover_l48_20190424 ADD COLUMN b integer;

UPDATE nlcd.lookup_nlcd_2011_land_cover_l48_20190424 set b=1 WHERE value NOT in (0,81,82) AND count <> 0;

UPDATE nlcd.lookup_nlcd_2011_land_cover_l48_20190424 set b=0 WHERE value in (81,82);

UPDATE nlcd.lookup_nlcd_2011_land_cover_l48_20190424 set b=255 WHERE value = 0;


--QAQC to compare with raster statistics
SELECT 
  SUM(count) 
FROM 
  nlcd.lookup_nlcd_2011_land_cover_l48_20190424
WHERE count IS NOT NULL AND value not in (81,82);



--------------------------------------------
--- nlcd.lookup_nlcd_2013_land_cover_l48_20190424 ----------------
--------------------------------------------
SELECT 
  * 
FROM 
  nlcd.lookup_nlcd_2013_land_cover_l48_20190424
WHERE count <> 0;

ALTER TABLE nlcd.lookup_nlcd_2013_land_cover_l48_20190424 ADD COLUMN b integer;

UPDATE nlcd.lookup_nlcd_2013_land_cover_l48_20190424 set b=1 WHERE value NOT in (0,81,82) AND count <> 0;

UPDATE nlcd.lookup_nlcd_2013_land_cover_l48_20190424 set b=0 WHERE value in (81,82);

UPDATE nlcd.lookup_nlcd_2013_land_cover_l48_20190424 set b=255 WHERE value = 0;


--QAQC to compare with raster statistics
SELECT 
  SUM(count) 
FROM 
  nlcd.lookup_nlcd_2013_land_cover_l48_20190424
WHERE count IS NOT NULL AND value not in (81,82);



--------------------------------------------
--- nlcd.lookup_nlcd_2016_land_cover_l48_20190424 ----------------
--------------------------------------------
SELECT 
  * 
FROM 
  nlcd.lookup_nlcd_2016_land_cover_l48_20190424
WHERE count <> 0;

ALTER TABLE nlcd.lookup_nlcd_2016_land_cover_l48_20190424 ADD COLUMN b integer;

UPDATE nlcd.lookup_nlcd_2016_land_cover_l48_20190424 set b=1 WHERE value NOT in (0,81,82) AND count <> 0;

UPDATE nlcd.lookup_nlcd_2016_land_cover_l48_20190424 set b=0 WHERE value in (81,82);

UPDATE nlcd.lookup_nlcd_2016_land_cover_l48_20190424 set b=255 WHERE value = 0;


--QAQC to compare with raster statistics
SELECT 
  SUM(count) 
FROM 
  nlcd.lookup_nlcd_2016_land_cover_l48_20190424
WHERE count IS NOT NULL AND value not in (81,82);