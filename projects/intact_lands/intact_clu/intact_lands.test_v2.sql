--CREATE TABLE intact_clu.test_v2 as 

SELECT 
  states.atlas_name as state,
  counties.atlas_stco,
  counties.atlas_name as county,
  clu_2003_crop_histo.acres as acres,
  2003 as year
FROM
  clu_2003.clu_2003_crop_histo FULL OUTER JOIN spatial.counties USING(atlas_stco) FULL OUTER JOIN spatial.states ON LEFT(counties.atlas_stco,2)=states.atlas_st

UNION

SELECT 
  states.atlas_name as state,
  counties.atlas_stco,
  counties.atlas_name as county,
  clu_2004_crop_histo.acres as acres,
  2004 as year
FROM
  clu_2004.clu_2004_crop_histo FULL OUTER JOIN spatial.counties USING(atlas_stco) FULL OUTER JOIN spatial.states ON LEFT(counties.atlas_stco,2)=states.atlas_st


UNION

SELECT 
  states.atlas_name as state,
  counties.atlas_stco,
  counties.atlas_name as county,
  clu_2005_crop_histo.acres as acres,
  2005 as year
FROM
  clu_2005.clu_2005_crop_histo FULL OUTER JOIN spatial.counties USING(atlas_stco) FULL OUTER JOIN spatial.states ON LEFT(counties.atlas_stco,2)=states.atlas_st


UNION

SELECT 
  states.atlas_name as state,
  counties.atlas_stco,
  counties.atlas_name as county,
  clu_2006_crop_histo.acres as acres,
  2006 as year
FROM
  clu_2006.clu_2006_crop_histo FULL OUTER JOIN spatial.counties USING(atlas_stco) FULL OUTER JOIN spatial.states ON LEFT(counties.atlas_stco,2)=states.atlas_st



UNION

SELECT 
  states.atlas_name as state,
  counties.atlas_stco,
  counties.atlas_name as county,
  clu_2007_crop_histo.acres as acres,
  2007 as year
FROM
  clu_2007.clu_2007_crop_histo FULL OUTER JOIN spatial.counties USING(atlas_stco) FULL OUTER JOIN spatial.states ON LEFT(counties.atlas_stco,2)=states.atlas_st


UNION

SELECT 
  states.atlas_name as state,
  counties.atlas_stco,
  counties.atlas_name as county,
  clu_2008_crop_histo.acres as acres,
  2008 as year
FROM
  clu_2008.clu_2008_crop_histo FULL OUTER JOIN spatial.counties USING(atlas_stco) FULL OUTER JOIN spatial.states ON LEFT(counties.atlas_stco,2)=states.atlas_st

UNION

SELECT 
  states.atlas_name as state,
  counties.atlas_stco,
  counties.atlas_name as county,
  clu_2009_crop_histo.acres as acres,
  2009 as year
FROM
  clu_2009.clu_2009_crop_histo FULL OUTER JOIN spatial.counties USING(atlas_stco) FULL OUTER JOIN spatial.states ON LEFT(counties.atlas_stco,2)=states.atlas_st

UNION

SELECT 
  states.atlas_name as state,
  counties.atlas_stco,
  counties.atlas_name as county,
  clu_2011_crop_histo.acres as acres,
  20011 as year
FROM
  clu_2011.clu_2011_crop_histo FULL OUTER JOIN spatial.counties USING(atlas_stco) FULL OUTER JOIN spatial.states ON LEFT(counties.atlas_stco,2)=states.atlas_st

UNION

SELECT 
  states.atlas_name as state,
  counties.atlas_stco,
  counties.atlas_name as county,
  clu_2012_crop_histo.acres as acres,
  20012 as year
FROM
  clu_2012.clu_2012_crop_histo FULL OUTER JOIN spatial.counties USING(atlas_stco) FULL OUTER JOIN spatial.states ON LEFT(counties.atlas_stco,2)=states.atlas_st

UNION

SELECT 
  states.atlas_name as state,
  counties.atlas_stco,
  counties.atlas_name as county,
  clu_2013_crop_histo.acres as acres,
  20013 as year
FROM
  clu_2013.clu_2013_crop_histo FULL OUTER JOIN spatial.counties USING(atlas_stco) FULL OUTER JOIN spatial.states ON LEFT(counties.atlas_stco,2)=states.atlas_st


UNION

SELECT 
  states.atlas_name as state,
  counties.atlas_stco,
  counties.atlas_name as county,
  clu_2014_crop_histo.acres as acres,
  20014 as year
FROM
  clu_2014.clu_2014_crop_histo FULL OUTER JOIN spatial.counties USING(atlas_stco) FULL OUTER JOIN spatial.states ON LEFT(counties.atlas_stco,2)=states.atlas_st

UNION

SELECT 
  states.atlas_name as state,
  counties.atlas_stco,
  counties.atlas_name as county,
  clu_2015_crop_histo.acres as acres,
  20015 as year
FROM
  clu_2015.clu_2015_crop_histo FULL OUTER JOIN spatial.counties USING(atlas_stco) FULL OUTER JOIN spatial.states ON LEFT(counties.atlas_stco,2)=states.atlas_st

order by state, county, year























SELECT 
  states.atlas_name as state,
  counties.atlas_stco,
  counties.atlas_name,
  clu_2003_crop_histo.acres as acres_2003,
  clu_2004_crop_c_histo.acres as acres_2004,
  clu_2005_crop_c_histo.acres as acres_2005,
  clu_2006_crop_c_histo.acres as acres_2006,
  clu_2007_crop_c_histo.acres as acres_2007,
  clu_2008_crop_c_histo.acres as acres_2008,
  clu_2009_crop_c_histo.acres as acres_2009,
  clu_2011_crop_c_histo.acres as acres_2011
  --clu_2012_crop_c_histo.acres as acres_2012,
  --clu_2013_crop_c_histo.acres as acres_2013,
  --clu_2014_crop_c_histo.acres as acres_2014,
  --clu_2015_crop_c_histo.acres as acres_2015
FROM
  clu_2003.clu_2003_crop_histo FULL OUTER JOIN clu_2004.clu_2004_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2005.clu_2005_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2006.clu_2006_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2007.clu_2007_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2008.clu_2008_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2009.clu_2009_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2011.clu_2011_crop_c_histo USING(atlas_stco)
                               --FULL OUTER JOIN clu_2012.clu_2012_crop_c_histo USING(atlas_stco)
                               --FULL OUTER JOIN clu_2013.clu_2013_crop_c_histo USING(atlas_stco)
                               --FULL OUTER JOIN clu_2014.clu_2014_crop_c_histo USING(atlas_stco)
                               --FULL OUTER JOIN clu_2015.clu_2015_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN spatial.counties USING(atlas_stco)
                               FULL OUTER JOIN spatial.states ON LEFT(atlas_stco,2) = states.atlas_st
WHERE states.atlas_name in ('Minnesota', 'Iowa', 'Nebraska', 'South Dakota', 'North Dakota', 'Montana', 'Wyoming')







-----NEW--------------------------------------
CREATE TABLE intact_clu.test_v3 as 
SELECT 
  states.atlas_name as state,
  counties.atlas_stco,
  counties.atlas_name,
  clu_2004_crop_c_histo.acres - clu_2003_crop_histo.acres as ytc_2004,
  clu_2005_crop_c_histo.acres - clu_2004_crop_c_histo.acres as ytc_2005,
  clu_2006_crop_c_histo.acres - clu_2005_crop_c_histo.acres as ytc_2006,
  clu_2007_crop_c_histo.acres - clu_2006_crop_c_histo.acres as ytc_2007,
  clu_2008_crop_c_histo.acres - clu_2007_crop_c_histo.acres as ytc_2008,
  clu_2009_crop_c_histo.acres - clu_2008_crop_c_histo.acres as ytc_2009,
  clu_2011_crop_c_histo.acres - clu_2009_crop_c_histo.acres as ytc_2011,
  clu_2012_crop_c_histo.acres - clu_2011_crop_c_histo.acres as ytc_2012,
  clu_2013_crop_c_histo.acres - clu_2012_crop_c_histo.acres as ytc_2013,
  clu_2014_crop_c_histo.acres - clu_2013_crop_c_histo.acres as ytc_2014,
  clu_2015_crop_c_histo.acres - clu_2014_crop_c_histo.acres as ytc_2015
FROM
  clu_2003.clu_2003_crop_histo FULL OUTER JOIN clu_2004.clu_2004_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2005.clu_2005_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2006.clu_2006_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2007.clu_2007_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2008.clu_2008_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2009.clu_2009_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2011.clu_2011_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2012.clu_2012_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2013.clu_2013_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2014.clu_2014_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2015.clu_2015_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN spatial.counties USING(atlas_stco)
                               FULL OUTER JOIN spatial.states ON LEFT(atlas_stco,2) = states.atlas_st
WHERE states.atlas_name in ('Minnesota', 'Iowa', 'Nebraska', 'South Dakota', 'North Dakota', 'Montana', 'Wyoming')






CREATE TABLE intact_clu.test_v3_State as 
SELECT 
  states.atlas_name as state,
 -- counties.atlas_stco,
  --counties.atlas_name,
  --clu_2004_crop_c_histo.acres - clu_2003_crop_histo.acres as ytc_2004,
  --clu_2005_crop_c_histo.acres - clu_2004_crop_c_histo.acres as ytc_2005,
  --clu_2006_crop_c_histo.acres - clu_2005_crop_c_histo.acres as ytc_2006,
 -- clu_2007_crop_c_histo.acres - clu_2006_crop_c_histo.acres as ytc_2007,
 -- clu_2008_crop_c_histo.acres - clu_2007_crop_c_histo.acres as ytc_2008,
 -- clu_2009_crop_c_histo.acres - clu_2008_crop_c_histo.acres as ytc_2009,
  --clu_2011_crop_c_histo.acres - clu_2009_crop_c_histo.acres as ytc_2011,
  sum(clu_2012_crop_c_histo.acres - clu_2011_crop_c_histo.acres) as ytc_2012,
  sum(clu_2013_crop_c_histo.acres - clu_2012_crop_c_histo.acres) as ytc_2013,
  sum(clu_2014_crop_c_histo.acres - clu_2013_crop_c_histo.acres) as ytc_2014,
  sum(clu_2015_crop_c_histo.acres - clu_2014_crop_c_histo.acres) as ytc_2015
FROM
  clu_2003.clu_2003_crop_histo FULL OUTER JOIN clu_2004.clu_2004_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2005.clu_2005_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2006.clu_2006_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2007.clu_2007_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2008.clu_2008_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2009.clu_2009_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2011.clu_2011_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2012.clu_2012_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2013.clu_2013_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2014.clu_2014_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN clu_2015.clu_2015_crop_c_histo USING(atlas_stco)
                               FULL OUTER JOIN spatial.counties USING(atlas_stco)
                               FULL OUTER JOIN spatial.states ON LEFT(atlas_stco,2) = states.atlas_st
WHERE states.atlas_name in ('Minnesota', 'Iowa', 'Nebraska', 'South Dakota', 'North Dakota', 'Montana', 'Wyoming')

GROUP BY states.atlas_name
