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

formatAC<-function(x){x/1000000}

df <- dbGetQuery(con, "SELECT years,acres,series,yxc,series_order FROM counts_yxc.merged_series WHERE series = 's35'")

ggplot(df, aes(x=years, y=acres, group=yxc, color=yxc, ordered = TRUE)) +
  geom_line(size=0.80) +
  scale_y_continuous(labels=formatAC) +
  scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016)) +
  labs(y="Acreage in Millions",x="Years")+
  ggtitle('S35 Compare YTC and YFC') + theme(plot.title = element_text(hjust = 0.5)) +
  theme(aspect.ratio=0.5, legend.title=element_blank(), legend.position="bottom") ##this creates 1 to 1 aspect ratio so when export to pdf not stretched



