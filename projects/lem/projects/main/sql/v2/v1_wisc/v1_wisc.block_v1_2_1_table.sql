CREATE TABLE v1.block_v1_2_1_table AS 
 SELECT main.geoid,
    LEFT(main.geoid,12) as block_group,
    st_area(main.wkb_geometry) * 0.000247105::double precision AS acres,
    neighbors.neighbor_list,
    hybrid.majority AS luc,
    st_x(st_transform(st_centroid(main.wkb_geometry), 4152)) AS lng,
    st_y(st_transform(st_centroid(main.wkb_geometry), 4152)) AS lat,
    main.wkb_geometry AS geom
   FROM v1.block main
   --------------------------------------------------------------------------------
   --- derive neighbors column-----------------------------------------------------
   --------------------------------------------------------------------------------
     JOIN ( SELECT a.src_geoid,
            string_agg(a.nbr_geoid, ', '::text) AS neighbor_list
           FROM v1.block_neighbors a
          WHERE a.length <> 0::double precision
          GROUP BY a.src_geoid) neighbors ON main.geoid::text = neighbors.src_geoid
    -------------------------------------------------------------------------------
    --- derive majority column-----------------------------------------------------
    -------------------------------------------------------------------------------
     JOIN ( SELECT block_zonal_maj_v1_1_1.geoid,
             block_zonal_maj_v1_1_1.majority
            FROM 
             v1.block_zonal_maj_v1_1_1
            WHERE NOT (block_zonal_maj_v1_1_1.geoid IN ( SELECT block_zonal_maj_v1_0.geoid
                                                         FROM v1.block_zonal_maj_v1_0
                                                         WHERE block_zonal_maj_v1_0.majority = 11))
	   UNION
	  
	   SELECT block_zonal_maj_v1_0.geoid,
	    nwalt_lookup.grouped AS majority
	   FROM v1.block_zonal_maj_v1_0,
	    nwalt_lookup
	   WHERE block_zonal_maj_v1_0.majority = nwalt_lookup.initial AND block_zonal_maj_v1_0.majority = 11) hybrid ON hybrid.geoid = main.geoid::text
    ---------------------------------------------------------------------------------
    --- QAQC constrain records to maintain Foreign Key constaints--------------------
    ---------------------------------------------------------------------------------    
    JOIN (SELECT 
	  block_group.geoid
	FROM 
	  v1.block_group, 
	(SELECT 
	left(block.geoid,12) as grouped_geoid
	FROM 
	v1.block
	group by 
	left(block.geoid,12)) as blocks
	WHERE 
	  block_group.geoid = blocks.grouped_geoid) as qaqc ON qaqc.geoid = LEFT(main.geoid,12)


