library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
library(radiant.data)
library(Hmisc)


drv <- dbDriver("PostgreSQL")



# con <- dbConnect(drv, dbname = "usxp_deliverables",
#                   host = "144.92.235.105", port = 5432,
#                   user = "mbougie", password = "Mend0ta!")


df <- dbGetQuery(con,"SELECT value::numeric, count::numeric FROM slope.s35_slope_null ")

print(df)


wm <- weighted.mean(df$value, df$count);
wsd <- wtd.sd(df$value, df$count)

print(wm)
print(wsd)




