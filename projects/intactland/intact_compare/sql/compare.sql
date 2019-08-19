

---clu/nlcd same
---2: 15,1,0
---5: 15,1,1


---clu/nlcd diffrent
---3: 0,1,0
---4;15,0,0
---6: 0,1,1
---7:15,0,1


SELECT
ROUND(((overlap.acres/total.acres) * 100)::numeric,0) as perc,
ST_Transform(counties.wkb_geometry,4326) as geom

FROM
(SELECT 
  atlas_stco,
  sum(acres) as acres
FROM 
  intact_compare.intact_compare_hist_counties 
WHERE label IN ('2','5')
GROUP BY atlas_stco
HAVING SUM(acres) <> 0) as overlap


INNER JOIN 


(SELECT 
 atlas_stco,
 sum(acres) as acres
FROM 
  intact_compare.intact_compare_hist_counties 
WHERE label IN ('2','3','4','5','6','7')
GROUP BY atlas_stco
HAVING SUM(acres) <> 0) as total

USING(atlas_stco)

INNER JOIN

spatial.counties_102003 as counties

USING(atlas_stco)








---clu/pete same
---5: 15,1,1
---7: 15,0,1


---clu/pete diffrent
---2:15,1,0
---4;15,0,0
---6:0,1,1
---8:0,0,1


SELECT 
yo.perc,
yo.geom
FROM


(SELECT
ROUND(((overlap.acres/total.acres) * 100)::numeric,0) as perc,
ST_Transform(counties.wkb_geometry,4326) as geom

FROM
(SELECT 
  atlas_stco,
  sum(acres) as acres
FROM 
  intact_compare.intact_compare_hist_counties 
WHERE label IN ('5','7')
GROUP BY atlas_stco
HAVING SUM(acres) <> 0) as overlap


INNER JOIN 


(SELECT 
 atlas_stco,
 sum(acres) as acres
FROM 
  intact_compare.intact_compare_hist_counties 
WHERE label IN ('2','4','5','6','7','8')
GROUP BY atlas_stco
HAVING SUM(acres) <> 0) as total

USING(atlas_stco)

INNER JOIN

spatial.counties_102003 as counties

USING(atlas_stco)) as yo

WHERE perc > 2