
 SELECT main.geoid,
    lookup.grouped AS luc,
    main.wkb_geometry AS geom
   FROM v1_wisc.block main
     JOIN v1_wisc.block_zonal_maj_v1_1_1 zonal ON main.geoid::text = zonal.geoid
     JOIN nwalt_lookup lookup ON lookup.grouped = zonal.majority;
