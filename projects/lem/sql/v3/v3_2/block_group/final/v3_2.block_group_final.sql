----Query returned successfully: 215833 rows affected, 111079 ms execution time.

CREATE TABLE v3_2.block_group_final as 

--EXPLAIN

SELECT * FROM v3_2.block_group_final_core

UNION

SELECT * FROM v3_2.block_group_final_water

UNION

SELECT * FROM v3_2.block_group_final_bottom


