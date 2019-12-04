
-------------------  block  ------------------------------

---create index using geoid column
CREATE INDEX v3_2_block_geoid_idx ON v3_2.block (geoid);


-----zonal tables--------------------------------------------
---Query returned successfully with no result in 1544037 ms.  Note: this for all 3 zonal indexes
---create index using geoid column
CREATE INDEX v3_2_block_zonal_maj_nwalt_60m_geoid_idx ON v3_2.block_zonal_maj_nwalt_60m (geoid);

---create index using geoid column
CREATE INDEX v3_2_block_zonal_maj_nwalt_rc_60m_geoid_idx ON v3_2.block_zonal_maj_nwalt_rc_60m (geoid);

---create index using geoid column
CREATE INDEX v3_2_block_zonal_maj_biomes_60m_geoid_idx ON v3_2.block_zonal_maj_biomes_60m (geoid);




-------------------  block group  ------------------------------

---create index using geoid column
CREATE INDEX v3_2_block_group_geoid_idx ON v3_2.block_group (geoid);

---create index using geoid column
CREATE INDEX v3_2_block_group_zonal_maj_nwalt_geoid_idx ON v3_2.block_group_zonal_maj_nwalt (geoid);

---create index using geoid column
CREATE INDEX v3_2_block_group_zonal_maj_nwalt_rc_geoid_idx ON v3_2.block_group_zonal_maj_nwalt_rc (geoid);

---create index using geoid column
CREATE INDEX v3_2_block_group_zonal_maj_biomes_geoid_idx ON v3_2.block_group_zonal_maj_biomes (geoid);