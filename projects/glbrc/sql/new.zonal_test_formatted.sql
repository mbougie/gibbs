SELECT states.atlas_name,
                        sum(main.acres::integer) as acres,
                        sum((main.acres::integer)/100000) as acres_scale, 
                        lookup.landcover
                        FROM new.zonal_test as main 
                        join spatial.states on left(main.atlas_stco,2)=states.atlas_st 
                        join new.lookup using(label)
                        GROUP BY 
                        states.atlas_name,
                        lookup.landcover
                  