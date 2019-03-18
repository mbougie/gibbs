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
library(extrafont)

# loadfonts(device = "win")

#Get rid of anything saved in your workspace 
# rm(list=ls())


# 
# drv <- dbDriver("PostgreSQL")
# 
con <- dbConnect(drv, dbname = "usxp",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")

df <- dbGetQuery(con, "(SELECT b.group_name as name, 
                      b.group_color as color, 
                      sum(a.acres) as acres, 
                      a.year, 
                      c.st_abbrev as state,
                      a.state::integer as rank
                      FROM combine.s35_sm_ytc_fc a 
                      INNER JOIN misc.lookup_cdl b ON a.label=b.value 
                      INNER JOIN spatial.states as c ON a.state=c.atlas_st 
                      WHERE color IS NOT NULL AND group_name IN ('Corn', 'Soybeans', 'Alfalfa', 'Wheat', 'Cotton', 'Sorghum') 
                      GROUP BY b.group_name, b.group_color, a.year, c.st_abbrev, a.state order by year, name, state)
                 
                      UNION
                 
                      (SELECT b.group_name as name, 
                      b.group_color as color, 
                      ---NOTE:Divide acres by 10 to maintain scale consistency!!!
                      sum(a.acres)/10 as acres, 
                      a.year,
                      'CONUS'::text as state,
                      100::integer as rank
                      FROM combine.s35_sm_ytc_fc a 
                      INNER JOIN misc.lookup_cdl b ON a.label=b.value
                      WHERE color IS NOT NULL AND group_name IN ('Corn', 'Soybeans', 'Alfalfa', 'Wheat', 'Cotton', 'Sorghum') 
                      GROUP BY b.group_name, b.group_color, a.year order by year, name)")



formatAC<-function(x){x/1000000}  
  
##this reorders the labels in the legend in chronological order
df$name <- with(df, reorder(name, -acres))
jColors <- df$color
names(jColors) <- df$name
print(head(jColors))


df <- mutate(df, state = reorder(state, rank))
print(df$state)




plotResult<-ggplot(df, aes(x=year, y=acres, group=name, color=name, ordered = TRUE)) +
  geom_line(size=0.50) +
  facet_wrap(~ state) +
  # scale_linetype_manual(values=c("dashed"))+
  # scale_y_continuous(labels=formatAC) +
  scale_y_continuous(
    labels=formatAC,
    "Expansion(acres in mil)", 
    sec.axis = sec_axis(~ . * 1.20, name = "", labels=formatAC)
  )+
  scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016)) +
  labs(y="Expansion(acres in mil)",x="Years",color="none",caption="NOTE: CONUS conversion values were divided by 10 to maintain scale consistency.")+
  ggtitle('Selected Break-out Crops') + theme(plot.title = element_text(hjust = 0.5))+
  theme(strip.text.x = element_text(size = 7, margin = margin(1,0,1,0, "mm")),
        # aspect.ratio=0.5,
        # legend.title=element_blank(),
        # text=element_text(size=16,  family="TT Arial"),
        # legend.position = c(0.07, -0.35), ##this creates 1 to 1 aspect ratio so when export to pdf not stretched
        axis.ticks = element_blank(),
        axis.text.x = element_text(size=6,angle=90),
        axis.text.y = element_text(size=6,vjust=0),
        panel.grid.minor.x=element_blank(),
        plot.title = element_text(hjust = 0.5),
        legend.position ="None",
        plot.caption = element_text(hjust = 0.5))+
  scale_colour_manual(values = jColors)


plotResult
