library(data.table)
library(maptools)#R package with useful map tools
library(rgeos)#Geomegry Engine Open Source (GEOS)
library(rgdal)#Geospatial Data Analysis Library (GDAL)
library(ggplot2)
library(extrafont)
library(RColorBrewer)
library(plotly)
library(dplyr)
library(geofacet)
library(rgdal)# R wrapper around GDAL/OGR
require("RPostgreSQL")
library(dplyr)

#Get rid of anything saved in your workspace 
rm(list=ls())



drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "nass",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")


# df <- dbGetQuery(con, "SELECT label,merged_acres.state_alpha, merged_acres.year::integer, merged_acres.acres_yo as acres FROM counts.merged_acres INNER JOIN counts.merged_acres_lookup_t2 using(short_desc) WHERE label IS NOT NULL")
df <- dbGetQuery(con, "SELECT sum(acres) as acres,year::integer FROM counts.merged_acres a INNER JOIN counts.merged_acres_lookup b ON a.short_desc=b.short_desc WHERE label is NOT NULL AND year NOT IN ('2008','2017') GROUP BY year order by year")
# df <- dbGetQuery(con, "SELECT sum(acres_yo) as acres,label,state_alpha,year::integer FROM counts.merged_acres a INNER JOIN counts.merged_acres_lookup_t2 b ON a.short_desc=b.short_desc WHERE label IS NOT NULL GROUP BY label,state_alpha,year")

format_mil<-function(x){x/1000000}


####  important series   ##########################################################################################

###query the data from postgreSQL
# df <- dbGetQuery(con, "SELECT years,acres,series,yxc,series_order,series || ': ' || label_traj as label FROM counts_yxc.merged_series as a inner join series_meta.meta as b using(series)  where yxc = 'yfc' and series != 's21_seperate' ")
# df <- dbGetQuery(con, "SELECT years,acres,series,yxc,series_order FROM counts_yxc.merged_series where yxc = 'ytc' AND (series='s22' OR series='s27') ")


##this reorders the labels in the legend in chronological order
# df$series <- with(df, reorder(series, series_order))


plot = ggplot(df, aes(x=year, y=acres, ordered = TRUE)) +
  geom_line(size=0.40) +
  scale_linetype_manual(values=c("dashed"))+
  scale_y_continuous(labels=format_mil) +
  scale_x_continuous(breaks=c(2008,2009,2010,2011,2012,2013,2014,2015,2016,2017)) +
  labs(y="Acreage in Millions",x="Years")+
  ggtitle('NASS Planted') + theme(plot.title = element_text(hjust = 0.5)) +
  theme(aspect.ratio=0.5, legend.title=element_blank(), legend.position="bottom") ##this creates 1 to 1 aspect ratio so when export to pdf not stretched
# scale_colour_manual(values = c("#e41a1c", "#377eb8", "#4daf4a", "#984ea3", "#ff7f00", "#ff7f00"))

plot
# pdf("C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\post\\yxc\\r_code\\ytc_all.pdf")
# print(plot)
# dev.off()
