library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
drv <- dbDriver("PostgreSQL")


# 
con1 <- dbConnect(drv, dbname = "nri",
                  host = "144.92.235.105", port = 5432,
                  user = "mbougie", password = "Mend0ta!")
# 
df_nri <- dbGetQuery(con1, "SELECT st_abbrev as state, year::int, acres FROM main.gross_abandonment_year_state")
# 
# 
# 
con2 <- dbConnect(drv, dbname = "usxp",
                  host = "144.92.235.105", port = 5432,
                  user = "mbougie", password = "Mend0ta!")
# 
df_25 <- dbGetQuery(con2, "SELECT b.st_abbrev as state,a.year::int,a.acres::int FROM zonal_hist.s25_zonal_hist_table as a, spatial.states as b WHERE b.atlas_st = a.atlas_st;")

df_26 <- dbGetQuery(con2, "SELECT b.st_abbrev as state,a.year::int,a.acres::int FROM zonal_hist.s26_zonal_hist_table as a, spatial.states as b WHERE b.atlas_st = a.atlas_st;")

df_27 <- dbGetQuery(con2, "SELECT b.st_abbrev as state,a.year::int,a.acres::int FROM zonal_hist.s27_zonal_hist_table as a, spatial.states as b WHERE b.atlas_st = a.atlas_st;")

df_28 <- dbGetQuery(con2, "SELECT b.st_abbrev as state,a.year::int,a.acres::int FROM zonal_hist.s28_zonal_hist_table as a, spatial.states as b WHERE b.atlas_st = a.atlas_st;")

df_31 <- dbGetQuery(con2, "SELECT b.st_abbrev as state,a.year::int,a.acres::int FROM zonal_hist.s31_zonal_hist_table as a, spatial.states as b WHERE b.atlas_st = a.atlas_st;")

df_32 <- dbGetQuery(con2, "SELECT b.st_abbrev as state,a.year::int,a.acres::int FROM zonal_hist.s32_zonal_hist_table as a, spatial.states as b WHERE b.atlas_st = a.atlas_st;")

df_33 <- dbGetQuery(con2, "SELECT b.st_abbrev as state,a.year::int,a.acres::int FROM zonal_hist.s33_zonal_hist_table as a, spatial.states as b WHERE b.atlas_st = a.atlas_st;")

#Abandonment
formatAC<-function(x){x/1000000}
# color2011<-ifelse(df25$year==2010,"turquoise","black")


plotResult<-ggplot(NULL, aes(x=year, group=state)) +
  geom_line(data = df_nri, linetype="dashed", aes(y=acres,color="nri"),alpha=0.8)+
  geom_line(data = df_25, aes(y=acres,color="s25"),alpha=0.8)+
  # geom_line(data = df_26, aes(y=acres,color="s26"),alpha=0.8)+
  geom_line(data = df_27, aes(y=acres,color="s27"),alpha=0.8)+
  # geom_line(data = df_31, aes(y=acres,color="s31"),alpha=0.8)+
  geom_line(data = df_32, aes(y=acres,color="s32"),alpha=0.8)+
  geom_line(data = df_33, aes(y=acres,color="s33"),alpha=0.8)+
  # geom_line(data = df4, aes(y=acres,color="s28"),alpha=0.8)+
  # geom_line(data = df5, aes(y=acres,color="s32"),alpha=0.8)+
  # geom_vline(xintercept = 2009,color="turquoise")+
  # scale_y_continuous(sec.axis = sec_axis(~./50000,name="Percentage of National Contribution"),labels=formatAC)+
  scale_y_continuous(labels=formatAC)+
  scale_x_continuous(breaks=c(2008,2009,2010,2011,2012,2013,2014,2015,2016,2017))+
  scale_color_manual(values = c("black","#a6cee3","#1f78b4", "#4daf4a", "#e41a1c"))+
  labs(y="Acreage in Million",x="Years",color="",caption="")+
  facet_wrap(~ state)+
  ggtitle("Compare YFC and NRI Gross Abandonment")+
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

plotResult



