----Query returned successfully: 215834 rows affected, 310628 ms execution time.


CREATE TABLE v3_2.block_group_v3_main AS
SELECT
    main.objectid, 
    main.geoid,
    LEFT(main.geoid,12) as block_group_group,
    st_area(main.wkb_geometry) * 0.0001::double precision AS hectares,
    neighbors.neighbor_list,
    nwalt.majority AS nwalt,
    nwalt_rc.majority AS nwalt_rc,
    biomes.majority AS biomes,
    st_x(st_transform(st_centroid(main.wkb_geometry), 4152)) AS lng,
    st_y(st_transform(st_centroid(main.wkb_geometry), 4152)) AS lat,
    main.wkb_geometry AS geom
FROM v3_2.block_group main

LEFT JOIN v3_2.block_group_v3_neighbors as neighbors ON main.geoid = neighbors.src_geoid

---join nwalt dataset
LEFT JOIN v3_2.block_group_zonal_maj_nwalt_60m as nwalt ON main.geoid = nwalt.geoid

---join rc_nwalt dataset
LEFT JOIN v3_2.block_group_zonal_maj_nwalt_rc_60m as nwalt_rc ON main.geoid = nwalt_rc.geoid

---join biomes dataset
LEFT JOIN v3_2.block_group_zonal_maj_biomes_60m as biomes ON main.geoid = biomes.geoid



------create indexes after table is created-------------------------------------------
---create index using geoid column
CREATE INDEX v3_2_block_group_v2_main_geoid_idx ON v3_2.block_group_v3_main (geoid);

---create index using geom column
CREATE INDEX v3_2_block_group_v2_main_geom_idx ON v3_2.block_group_v3_main USING gist (geom);