CREATE OR REPLACE VIEW v1_wisc.block_v1_2_1_view AS 
 SELECT 
    ---main part of the query
    main.geoid,
    st_area(main.wkb_geometry) * 0.000247105::double precision AS acres,
    neighbors.neighbor_list,
    hybrid.majority AS luc,
    st_x(st_transform(st_centroid(main.wkb_geometry), 4152)) AS lng,
    st_y(st_transform(st_centroid(main.wkb_geometry), 4152)) AS lat,
    main.wkb_geometry AS geom
   FROM v1_wisc.block main
     --create neighbor_list
     JOIN ( SELECT a.src_geoid,
            string_agg(a.nbr_geoid, ', '::text) AS neighbor_list
           FROM v1_wisc.block_neighbors_v1_0 as a
          WHERE a.length <> 0::double precision
          GROUP BY a.src_geoid) neighbors ON main.geoid::text = neighbors.src_geoid
     --create zonal majority hybrid
     JOIN ( SELECT block_zonal_maj_v1_1_1.geoid,
            block_zonal_maj_v1_1_1.majority
           FROM v1_wisc.block_zonal_maj_v1_1_1
          WHERE NOT (block_zonal_maj_v1_1_1.geoid IN ( SELECT block_zonal_maj_v1_0.geoid
                   FROM v1_wisc.block_zonal_maj_v1_0
                  WHERE block_zonal_maj_v1_0.majority = 11))
        UNION
         SELECT block_zonal_maj_v1_0.geoid,
            nwalt_lookup.grouped AS majority
           FROM v1_wisc.block_zonal_maj_v1_0,
            nwalt_lookup
          WHERE block_zonal_maj_v1_0.majority = nwalt_lookup.initial AND block_zonal_maj_v1_0.majority = 11) AS hybrid ON hybrid.geoid=main.geoid;