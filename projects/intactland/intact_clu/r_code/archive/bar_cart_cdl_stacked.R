
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


formatAC<-function(x){x/100000}  

##### query the data from postgreSQL
df <- dbGetQuery(con, "SELECT 
                      b.st_abbrev, 
                      a.acres::integer as acres,
                      (acres::integer)/100000 as acres_scale, 
                      CASE
                      when value = '1' then 'forest'
                      when value = '2' then 'wetland'
                      when value = '3' then 'grassland'
                      when value = '4' then 'shrubland'
                      END landcover
                      FROM 
                        intact_clu.intactland_15_refined_cdl15_broad_hist_states as a INNER JOIN
                        spatial.states as b
                      ON
                        a.atlas_st = b.atlas_st")


df <- ddply(df, .(st_abbrev),transform, pos = cumsum(acres) - (0.5 * acres))


ggplot(df, aes(fill=factor(landcover, levels=c("wetland","shrubland","forest","grassland")), y=acres, x=reorder(st_abbrev, -acres))) + 
  geom_bar(stat="identity", width = 0.5)+
  scale_fill_manual(values = c("forest" = "#256e36", "shrubland" = "#9c140a", "wetland" = "#023c9f", "grassland" = "#a9d91a")) + 
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
