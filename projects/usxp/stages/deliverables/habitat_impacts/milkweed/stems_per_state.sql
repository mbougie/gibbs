/*I used the zonal histogram function which returns the value and count of milkweed raster per county.
In this query I multiply the value of the raster by it's acreage per county.
For example, if the value 3 occurred in 100 acres in Ohio then I multiplied 3 * 100 which indicates that there are this 300 stems contributing to the total stems in Ohio.
To get the total stems I took the sum of all the "value * acres" contributions per state.*/

SELECT 
  atlas_name as state,
  round(sum(label::integer * acres)::numeric,0) as total_stems_per_state
FROM 
  milkweed.s35_milkweed_state_hist INNER JOIN
  spatial.states ON s35_milkweed_state_hist.state = states.atlas_st
group by state,atlas_name

order by sum(label::integer * count)