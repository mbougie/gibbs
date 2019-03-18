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

con <- dbConnect(drv, dbname = "usxp",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")


# s35_ytc <- dbGetQuery(con2,"SELECT b.st_abbrev as state,a.year::int,a.acres::int, rank
#                             FROM zonal_hist.s35_zonal_hist_table_ytc as a, spatial.states as b 
#                             WHERE b.atlas_st = a.atlas_st
#                             
#                             UNION
#                             
#                             SELECT 
#                             'CONUS'::text as state,
#                             value as year,
#                             acres/10,
#                             100::integer as rank
#                             FROM 
#                             counts_yxc.s35_ytc30_2008to2017_mmu5")
# 


df <- dbGetQuery(con, "SELECT b.group_name as name, b.group_color as color, sum(a.acres) as acres, a.year, c.st_abbrev as state 
                       FROM  combine.s35_sm_ytc_fc a 
                       INNER JOIN misc.lookup_cdl b ON a.label=b.value 
                       INNER JOIN spatial.states as c ON a.state=c.atlas_st 
                       WHERE color IS NOT NULL AND group_name IN ('Corn', 'Soybeans', 'Alfalfa', 'Wheat', 'Cotton', 'Sorghum') GROUP BY b.group_name, b.group_color, a.year, c.st_abbrev order by year, name, state")



formatAC<-function(x){x/1000000}  
  
##this reorders the labels in the legend in chronological order
df$name <- with(df, reorder(name, -acres))
jColors <- df$color
names(jColors) <- df$name
print(head(jColors))

ggplot(df, aes(x=year, y=acres, group=name, color=name, ordered = TRUE)) +
  geom_line(size=0.50) +
  facet_wrap(~ state) +
  # scale_linetype_manual(values=c("dashed"))+
  scale_y_continuous(labels=formatAC) +
  scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016)) +
  labs(y="Acreage in Millions",x="Years")+
  ggtitle('Selected Crops Before Abandonment') + theme(plot.title = element_text(hjust = 0.5))+
  theme(aspect.ratio=0.5,
        legend.title=element_blank(),
        # legend.position = c(0.07, -0.35), ##this creates 1 to 1 aspect ratio so when export to pdf not stretched
        axis.text.x = element_text(size=6,angle=90))+
  scale_colour_manual(values = jColors)
