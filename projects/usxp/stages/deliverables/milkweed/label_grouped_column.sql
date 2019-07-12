UPDATE milkweed.s35_traj_bfc_fc 
SET grouped = 1
where value IN 
(SELECT 
  value 
FROM 
  milkweed.s35_traj_bfc_fc
WHERE s35_bfc IN (37,152,176) AND s35_fc IN (1,5));



UPDATE milkweed.s35_traj_bfc_fc 
SET grouped = 2
where value IN 
(SELECT 
  value 
FROM 
  milkweed.s35_traj_bfc_fc
WHERE s35_bfc IN (87,195) AND s35_fc IN (1,5));
