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
drv <- dbDriver("PostgreSQL")

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
    "Expansion (Millions of acres)", 
    sec.axis = sec_axis(~ ., name = "", labels=formatAC)
  )+
  scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016)) +
  labs(x="Years",color="none")+
  ggtitle('') + theme(plot.title = element_text(hjust = 0.5))+
  theme(strip.text.x = element_text(size = 7, margin = margin(1,0,1,0, "mm")),
        # aspect.ratio=0.5,
        # legend.title=element_blank(),
        # text=element_text(size=16,  family="TT Arial"),
        # legend.position = c(0.07, -0.35), ##this creates 1 to 1 aspect ratio so when export to pdf not stretched
        axis.ticks = element_blank(),

        axis.text.x = element_text(size=8, angle=90, margin = margin(t = 5, r = 0, b = 5, l = 0)),
        axis.text.y.left = element_text(size=8,vjust=0, margin = margin(t = 0, r = 5, b = 0, l = 5)),
        axis.text.y.right = element_text(size=8,vjust=0, margin = margin(t = 0, r = -5, b = 0, l = 5)),
        panel.grid.minor.x=element_blank(),
        plot.title = element_text(hjust = 0.5),
        
        #### legend stuff ###########
        # legend.position ="None",
        # legend.position = c(0.07, -0.35),
        legend.position = "right",
        legend.title = element_blank(),
        legend.key = element_rect(fill = "white")) + 
  guides(colour = guide_legend(override.aes = list(size=0.75))) + 
  scale_colour_manual(values = jColors)


plotResult



# ###create panel image ######################
dir = "C:\\Users\\Bougie\\Desktop\\temp\\"
fileout=paste(dir,"s35_yxc_sm",".png", sep="")
ggsave(fileout, width = 34, height = 25, dpi = 500)
