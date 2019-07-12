----Query returned successfully: 11007989 rows affected, 1590061 ms execution time.

CREATE TABLE v3_2.block_final as 

--EXPLAIN

SELECT * FROM v3_2.block_final_core

UNION

SELECT * FROM v3_2.block_final_water

UNION

SELECT * FROM v3_2.block_final_bottom


