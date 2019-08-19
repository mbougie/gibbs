
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
library(scales)
library(rjson)
# library(jsonlite)
require(RColorBrewer)
library(glue)



drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "intactland",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")



###NOTE: ALWYS CHECK THIS!!!!! ###############################
million = 10^6
hundered_thousand = 10^5

formatAC<-function(x){x/million}  





##### query the data from postgreSQL
df <- dbGetQuery(con,"SELECT 
                      b.st_abbrev, 
                      a.acres::integer as acres,
                      -----scale both intact acres
                      (a.acres::integer)/100000 as acres_scale,
                      ----create new column and convert the values to text
                      CASE
                      when a.value = '1' then 'forest'
                      when a.value = '2' then 'wetland'
                      when a.value = '3' then 'grassland'
                      when a.value = '4' then 'shrubland'
                      END landcover
                      -----------------------------------------------------
                      FROM 
                      intact_clu.intactland_15_refined_cdl15_broad_hist_states as a 
                      INNER JOIN
                      spatial.states as b
                      USING(atlas_st)
                      
                      
                      UNION
                      
                      
                      SELECT 
                      b.st_abbrev, 
                      a.acres::integer as acres,
                      -----scale both intact acres
                      (a.acres::integer)/100000 as acres_scale,
                      ----create new column and convert the values to text
                      CASE
                      when a.value = '0' then 'non-intact'
                      END landcover
                      -----------------------------------------------------
                      FROM 
                      intact_clu.intactland_15_refined_hist_states as a 
                      INNER JOIN
                      spatial.states as b
                      USING(atlas_st)
                      
                      ---this selects only nonintact values from intact_clu.intactland_15_refined_hist_states
                      WHERE a.label='0'")


df <- ddply(df, .(st_abbrev),transform, pos = cumsum(acres) - (0.5 * acres))


ggplot(df, aes(fill=factor(landcover, levels=c("wetland","shrubland","forest","grassland","non-intact")), y=acres, x=reorder(st_abbrev, -acres))) + 
  geom_bar(stat="identity", width = 0.5)+
  scale_fill_manual(values = c("forest" = "#256e36", "shrubland" = "#9c140a", "wetland" = "#023c9f", "grassland" = "#a9d91a", "non-intact" = "black")) + 
  # geom_text(aes(label = acres_scale), position = position_stack(vjust = 0.5)) +
  scale_x_discrete(name="states") +
  scale_y_continuous(labels=formatAC,"millions of acres") +
  guides(fill=guide_legend(title="landcover type",  title.theme = element_text(
    colour = "#707070", 
    size=15,
    vjust=0.0,
    angle = 0
  ))) + 
theme(  axis.ticks = element_blank(),
        axis.line = element_blank(),
        axis.text = element_text(colour = "#707070"),
        axis.title.x = element_text(colour = "#707070", size=rel(1)),
        axis.title.y = element_text(colour = "#707070", angle=90),
        panel.background = element_rect(fill="#E8E8E8"),
        panel.grid.major.y = element_line(size=1.25),
        panel.grid.minor.y = element_line(colour = "white"),
        panel.grid.major = element_line(colour = "white"),
        legend.text = element_text(colour="#707070", size = 12),
        plot.background = element_rect(fill="white"))
