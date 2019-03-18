CREATE TABLE choropleths.s35_perc_cov as 

SELECT 
  mtr1.label, 
  mtr1.atlas_stco,
  counties.atlas_caps, 
  (mtr3.acres/(mtr1.acres+ mtr3.acres)) * 100 as perc_conv,
  (mtr3.acres/acres_calc) * 100 as perc_conv,
  counties.geom
FROM 
  (SELECT 
  label, 
  atlas_stco, 
  acres
  FROM choropleths.test
  WHERE label='1') as mtr1
INNER JOIN
  (SELECT 
  label, 
  atlas_stco, 
  acres
  FROM choropleths.test
  WHERE label='3') as mtr3
ON 
  mtr1.atlas_stco = mtr3.atlas_stco 
INNER JOIN
  spatial.counties
ON 
  counties.atlas_stco = mtr1.atlas_stco 
order by (mtr3.acres/(mtr1.acres+ mtr3.acres)) * 100 DESC


