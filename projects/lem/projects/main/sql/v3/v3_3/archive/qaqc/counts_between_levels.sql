
---------------------------
----tract_final
----count:3108
----parent(county_final):3108
----difference from parent:0
---------------------------
----unique county geoids
SELECT
child.county,
parent.geoid
FROM
(SELECT 
  county
FROM 
  v3_2.tract_final
group by county) as child

INNER JOIN
v3_2.county_final as parent

ON parent.geoid = child.county


---------------------------
----block_group_final
----count:72265
----parent(tract_final):72268
----difference from parent:3
---------------------------
----unique tract geoids
SELECT
child.tract,
parent.geoid
FROM
(SELECT 
  tract
FROM 
  v3_2.block_group_final
group by tract) as child

INNER JOIN
v3_2.tract_final as parent
ON parent.geoid = child.tract


---------------------------
----block_final table
----count:
----parent(block_group_final):
----difference from parent:
---------------------------
----unique block geoids
SELECT
child.block_group,
parent.geoid
FROM
(SELECT 
  block_group
FROM 
  v3_2.block_final
group by block_group) as child

INNER JOIN
v3_2.block_group_final as parent
ON parent.geoid = child.block_group
