library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "intact_lands",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")

yo <- dbGetQuery(con, "SELECT * FROM public.nd_perc_diff;")


ggplot(data=yo, aes(x=year, y=percent_diff, group=atlas_stco, color=atlas_stco)) +
  geom_line()+
  facet_wrap(~ atlas_stco)+
  # labs(title = "Murder change between 2014 and 2015")+
  # scale_x_continuous(breaks=c(2004,2009))+
  theme(axis.ticks = element_blank(), axis.text.x = element_blank(), legend.position = "none")