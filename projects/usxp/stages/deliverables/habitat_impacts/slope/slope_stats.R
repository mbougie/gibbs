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
library(spatstat)
library(radiant)


drv <- dbDriver("PostgreSQL")



con <- dbConnect(drv, dbname = "usxp_deliverables",
                  host = "144.92.235.105", port = 5432,
                  user = "mbougie", password = "Mend0ta!")


df <- dbGetQuery(con,"SELECT value::numeric, count::numeric FROM slope.s35_slope_null Where value <= 90")

print(df)


wm <- weighted.mean(df$value, df$count);
wsd <- weighted.sd(df$value, df$count)
# 
# print(wm)
# print(wsd)


wmdn <- weighted.median(df$value, df$count)
wqauntile <- weighted.quantile(df$value, df$count)
