
 SELECT a.geoid,
    b.grouped AS luc,
    a.wkb_geometry AS geom
   FROM v1_wisc.block a
     JOIN ( SELECT block_zonal_maj_v1_1_1.geoid,
            nwalt_lookup.grouped
           FROM v1_wisc.block_zonal_maj_v1_1_1
             JOIN nwalt_lookup ON block_zonal_maj_v1_1_1.majority = nwalt_lookup.initial
          WHERE NOT (block_zonal_maj_v1_1_1.geoid IN ( SELECT block_zonal_maj_v1_0.geoid
                   FROM v1_wisc.block_zonal_maj_v1_0
                  WHERE block_zonal_maj_v1_0.majority = 11))
        UNION
         SELECT block_zonal_maj_v1_0.geoid,
            nwalt_lookup.grouped AS majority
           FROM v1_wisc.block_zonal_maj_v1_0,
            nwalt_lookup
          WHERE block_zonal_maj_v1_0.majority = nwalt_lookup.initial AND block_zonal_maj_v1_0.majority = 11) b USING (geoid);

