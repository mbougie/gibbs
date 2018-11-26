library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
drv <- dbDriver("PostgreSQL")



con2 <- dbConnect(drv, dbname = "usxp",
                  host = "144.92.235.105", port = 5432,
                  user = "mbougie", password = "Mend0ta!")

df_35_ytc <- dbGetQuery(con2, "SELECT b.st_abbrev as state,a.year::int,a.acres::int FROM zonal_hist.s35_zonal_hist_table_ytc as a, spatial.states as b WHERE b.atlas_st = a.atlas_st;")
df_35_yfc <- dbGetQuery(con2, "SELECT b.st_abbrev as state,a.year::int,a.acres::int FROM zonal_hist.s35_zonal_hist_table_yfc as a, spatial.states as b WHERE b.atlas_st = a.atlas_st;")


formatAC<-function(x){x/1000000}

ggplot(NULL, aes(x=year, group=state)) +

  geom_line(data = df_35_ytc, aes(y=acres,color="ytc"),alpha=0.8)+
  geom_line(data = df_35_yfc, aes(y=acres,color="yfc"),alpha=0.8)+
  scale_y_continuous(labels=formatAC)+
  scale_x_continuous(breaks=c(2008,2009,2010,2011,2012,2013,2014,2015,2016,2017))+
  labs(y="Acreage in Million",x="Years",color="",caption="")+
  facet_wrap(~ state)+
  ggtitle("Compare YTC and YFC Gross Abandonment")+
  theme(axis.ticks = element_blank(),
        axis.text.x = element_text(size=5,angle=90),
        axis.text.y = element_text(vjust=0),
        axis.title.y=element_text(color="black"),
        axis.title.y.right=element_text(color="turquoise"),
        panel.grid.minor.x=element_blank(),
        #panel.background = element_rect(size = 5),
        plot.title = element_text(hjust = 0.5),
        plot.caption = element_text(hjust = 0.5),
        legend.position ="bottom"
  )





