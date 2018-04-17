CREATE TABLE refinement_union.v4_traj_cdl30_b_2008to2017_rfnd_v3_states_composition as
SELECT 
  base.count as state_count, 
  base.traj_rfnd, 
  base.acres, 
  states.atlas_st, 
  states.st_abbrev, 
  lookup.traj_array, 
  lookup.ytc,
  (SELECT sum(v4_traj_cdl30_b_2008to2017_rfnd_v3_states.acres)
  FROM 
  refinement_union.v4_traj_cdl30_b_2008to2017_rfnd_v3_states, 
  pre.v4_traj_lookup_2008to2017_v3, 
  pre.v4_traj_cdl30_b_2008to2017, 
  spatial.states
WHERE 
  v4_traj_cdl30_b_2008to2017_rfnd_v3_states.traj_rfnd = v4_traj_cdl30_b_2008to2017."Value" AND
  v4_traj_cdl30_b_2008to2017.traj_array = v4_traj_lookup_2008to2017_v3.traj_array AND
  states.atlas_st = base.state
GROUP BY state,ytc
HAVING base.state = state AND lookup.ytc=ytc) as acres_state,
base.acres/(SELECT sum(v4_traj_cdl30_b_2008to2017_rfnd_v3_states.acres)
 FROM 
  refinement_union.v4_traj_cdl30_b_2008to2017_rfnd_v3_states, 
  pre.v4_traj_lookup_2008to2017_v3, 
  pre.v4_traj_cdl30_b_2008to2017, 
  spatial.states
WHERE 
  v4_traj_cdl30_b_2008to2017_rfnd_v3_states.traj_rfnd = v4_traj_cdl30_b_2008to2017."Value" AND
  v4_traj_cdl30_b_2008to2017.traj_array = v4_traj_lookup_2008to2017_v3.traj_array AND
  states.atlas_st = base.state
GROUP BY state,ytc
HAVING base.state = state AND lookup.ytc=ytc)  * 100 as percent_state
  
FROM 
  refinement_union.v4_traj_cdl30_b_2008to2017_rfnd_v3_states as base, 
  pre.v4_traj_lookup_2008to2017_v3 as lookup, 
  pre.v4_traj_cdl30_b_2008to2017, 
  spatial.states 
WHERE 
  base.traj_rfnd = v4_traj_cdl30_b_2008to2017."Value" AND
  v4_traj_cdl30_b_2008to2017.traj_array = lookup.traj_array AND
  states.atlas_st = base.state AND ytc is not null