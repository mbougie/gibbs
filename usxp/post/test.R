library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "usxp",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")


# murders_separate <- read.csv(file="test_yo.csv")
## query the data from postgreSQL 
murders_separate <- dbGetQuery(con, "SELECT value as years,acres,series,yxc FROM counts.merged_series where series = 's21_seperate' or series = 's20' ")


ggplot(murders_separate, aes(x=years, y=acres, group=interaction(series, yxc), color=interaction(series, yxc))) +
  geom_line()+
  scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016))







