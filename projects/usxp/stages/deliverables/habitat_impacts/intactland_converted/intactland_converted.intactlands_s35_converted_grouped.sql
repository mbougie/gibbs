CREATE TABLE intactland_converted.intactlands_s35_converted_grouped as
SELECT
group_class,
sum(count) as count,
sum(acres) as acres
FROM(
SELECT 
	value,
	CASE
	WHEN value IN (63,141,142,143) THEN 'forest'
	WHEN value IN (83,87,190,195) THEN 'wetland'
	WHEN value IN (37,62,171,176,181) THEN 'grassland'
	WHEN value IN (64,65,131,152) THEN 'shrubland'
	ELSE 'other'
	END AS group_class,
	count,
	count * 0.222395 as acres
FROM 
intactland_converted.intactlands_s35_converted
ORDER BY group_class) as reclassed_table
GROUP BY group_class
HAVING group_class IS NOT NULL
ORDER BY sum(acres) DESC