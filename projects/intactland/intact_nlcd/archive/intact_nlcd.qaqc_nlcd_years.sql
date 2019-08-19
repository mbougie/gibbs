--create table intact_nlcd.qaqc_nlcd_years as 

SELECT 
distinct
  a.nlcd30_1992, 
  b.nlcd30_2001, 
  c.nlcd30_2006, 
  d.nlcd30_2011


FROM
 
(SELECT 
distinct
  nlcd30_1992
FROM 
  intact_nlcd.nlcd_combine) as a

FULL OUTER JOIN 

(SELECT 
distinct
  nlcd30_2001
FROM 
  intact_nlcd.nlcd_combine) as b

ON a.nlcd30_1992 = b.nlcd30_2001

FULL OUTER JOIN 

  (SELECT 
distinct
  nlcd30_2006
FROM 
  intact_nlcd.nlcd_combine) as c

ON a.nlcd30_1992 = c.nlcd30_2006

FULL OUTER JOIN

(SELECT 
distinct
  nlcd30_2011
FROM 
  intact_nlcd.nlcd_combine) as d

ON a.nlcd30_1992 = d.nlcd30_2011

order by a.nlcd30_1992

