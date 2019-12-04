


----check between block and block_group------------
----Query returned successfully: 216399 rows affected, 808949 ms execution time.
CREATE TABLE v3_2.qaqc_levels_check___blk_blk_group AS

SELECT
    from_block.geoid as from_block,
    block_group.geoid as from_block_group,
    block_group.wkb_geometry as block_group_geom,
    block.wkb_geometry as block_geom

FROM
 
(SELECT
DISTINCT
LEFT(geoid,12) as geoid
FROM v3_2.block) as from_block

FULL OUTER JOIN v3_2.block_group ON from_block.geoid = block_group.geoid

----this attaches the block geometry to the dataset so can explore why from_block_group column is null while from_block column has a value
FULL OUTER JOIN v3_2.block ON from_block.geoid = LEFT(block.geoid,12)