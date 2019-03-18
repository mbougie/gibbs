SELECT 
  sum(potential_biofuels.count) * 0.222395 

FROM 
  marginal.potential_biofuels
WHERE cdl30_2017 IN (37,176) AND gssurgo_muaggatt IN (5,6,7,8) AND gssurgo_slopegra <= 20