CREATE table v2.tract_v2_table AS 
   SELECT 
    main.geoid,
    st_area(main.wkb_geometry) * 0.000247105::double precision AS acres,
    neighbors.neighbor_list,
    hybrid.majority AS luc,
    biomes.majority AS biomes,
    st_x(st_transform(st_centroid(main.wkb_geometry), 4152)) AS lng,
    st_y(st_transform(st_centroid(main.wkb_geometry), 4152)) AS lat,
    main.wkb_geometry AS geom

   FROM v2.tract AS main

   --neighbors query------------------------------------------------------------------
   LEFT JOIN ( SELECT 
                a.src_geoid,
                string_agg(a.nbr_geoid, ', '::text) AS neighbor_list
               FROM 
                v2.tract_neighbors a
               WHERE a.length <> 0::double precision
               GROUP BY a.src_geoid) AS neighbors 

   ON main.geoid::text = neighbors.src_geoid

   --LUC hybrid query--------------------------------------------------------------------
   --NOTE: for this hybrid dataset I am creating a hybrid dataset from the two datasets: tract_zonal_maj_rc_nwalt and tract_zonal_maj_nwalt.  I am doing this by removing the poygons in v1_1_1 that have the same geoid as
   --v1_0 had with majority 11 and then replacing these polgons with these poygons from v1_0.


   LEFT JOIN (--Note: this query selects all polygons from tract_zonal_maj_rc_nwalt dataset that were NOT labeled majority 11 in tract_zonal_maj_nwalt
              --Did this in order to remove these polygons and replace them with the polygons from query below!!
               SELECT 
                tract_zonal_maj_rc_nwalt.geoid,
                tract_zonal_maj_rc_nwalt.majority
               FROM 
                v2.tract_zonal_maj_rc_nwalt
               WHERE NOT (tract_zonal_maj_rc_nwalt.geoid IN ( SELECT tract_zonal_maj_nwalt.geoid
                                                                  FROM v2.tract_zonal_maj_nwalt
                                                                  WHERE tract_zonal_maj_nwalt.majority = 11))
	       UNION
	       
               (--Note: this query add all polygons from tract_zonal_maj_nwalt dataset that were labeled majority 11
                --Did this in order to add these polygons to the analysis
                SELECT 
                 tract_zonal_maj_nwalt.geoid,
                 nwalt_lookup.grouped AS majority

                FROM 
                 v2.tract_zonal_maj_nwalt JOIN
                 nwalt_lookup
                ON tract_zonal_maj_nwalt.majority = nwalt_lookup.initial 

                WHERE tract_zonal_maj_nwalt.majority = 11)) AS hybrid 

    ON main.geoid::text = hybrid.geoid


    ---join biomes dataset
    LEFT JOIN v2.tract_zonal_maj_biomes as biomes ON main.geoid::text = biomes.geoid;


