
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
library(scales)
drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "side_projects",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")



##########################  using supplied county dataset######################################   

##########################  using supplied county dataset######################################   
# state <- map_data("state") 

## query the data from postgreSQL 
df <- dbGetQuery(con, 'SELECT * FROM  mapbiomas.combine_sc_22 where "VALUE"!=9 AND "VALUE"!=146 order by "COUNT" desc limit 20')
df$VALUE <- with(df, reorder(VALUE, -COUNT))


p <-ggplot(df, aes(VALUE, COUNT, label = VALUE))
p +geom_bar(stat = "identity", aes(fill = VALUE)) + geom_text(check_overlap = TRUE)