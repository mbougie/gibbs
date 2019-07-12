SELECT 
  test_v2.state, 
  sum(test_v2.acres) acres, 
  test_v2.year
FROM 
  intact_clu.test_v2
WHERE state in ('Minnesota', 'Iowa', 'Nebraska', 'South Dakota', 'North Dakota', 'Montana', 'Wyoming')
group by
  test_v2.state, 
  test_v2.year

Having sum(test_v2.acres) is not null AND year > 2010


order by state, year