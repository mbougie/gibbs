----Query returned successfully: 3602949 rows affected, 5741 ms execution time.

---create table intensification_ksu.test as  
SELECT 
  *,
  ST_Transform(ST_SetSRID(ST_MakePoint(lon, lat),4152),4326) as geom
FROM 
  intensification_ksu.rfs_intensification_csv
limit 100


----add geometry column
SELECT AddGeometryColumn ('intensification_ksu','rfs_intensification_csv','geom',4152,'POINT',2);

----populate geom column
UPDATE intensification_ksu.rfs_intensification_csv SET
  geom = ST_SetSRID(ST_MakePoint(lon, lat),4152);




SELECT AddGeometryColumn ('intensification_ksu','rfs_intensification_csv','geom_4326',4326,'POINT',2);
----populate geom column
UPDATE intensification_ksu.rfs_intensification_csv SET
  geom_4326 = ST_Transform(ST_SetSRID(ST_MakePoint(lon, lat),4152),4326);