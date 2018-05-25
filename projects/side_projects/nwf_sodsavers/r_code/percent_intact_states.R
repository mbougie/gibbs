
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
library(scales)
drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "side_projects",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")



##########################  using supplied county dataset######################################   

##########################  using supplied county dataset######################################   
state <- map_data("state") 

## query the data from postgreSQL 
yo <- dbGetQuery(con, "SELECT lower(atlas_name) as region, sum(percent) percent FROM counts.grasslandhaywetland2011intact GROUP BY atlas_name order by percent")

## merge cnty2 with yo
d = merge(state, yo, sort = TRUE, by='region')

## If I don't reorder the order of lat long it "tears" the polygons!
state_fnl<-d[order(d$order),]


cnames <- aggregate(cbind(long, lat) ~ percent, data=state, 
                    FUN=function(x)mean(range(x)))



d = ggplot() + 
  geom_polygon(
    data=state_fnl,
    aes(y=lat, x=long, group=group, fill = percent),
    colour = 'grey50',
    size = 0.25
  ) + 
  scale_fill_distiller(name="% of total area by state", palette = "Greens", breaks = pretty_breaks(n = 5), trans = "reverse")+
  geom_polygon(
    data=state,
    aes(y=lat, x=long, group=group),
    fill='grey70',
    alpha=0,
    colour='white',
    size=0.9
  )+
  coord_map(project="polyconic") +
  labs(
    title = "Remaining Intact (Native) Lands"
    # subtitle = "Grassland/Pastur"
  ) +
  theme(
    text = element_text(color = "#22211d"), 
    axis.text.x = element_blank(),
    axis.title.x=element_blank(),
    axis.text.y = element_blank(),
    axis.title.y=element_blank(),
    axis.ticks = element_blank(),
    panel.grid.major = element_blank(),
    plot.background = element_rect(fill = "white", color = NA), 
    panel.background = element_rect(fill = "white", color = NA), 
    legend.background = element_rect(fill = "white", color = NA),
    plot.title = element_text(size= 15, hjust=0.21, color = "#4e4d47", margin = margin(b = -0.7, t = 0.1, l = 0.01, unit = "cm")),
    plot.subtitle = element_text(size= 10, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.20, l = 2, unit = "cm")),
    legend.position = c(0.23, 0.12)
  )

d + guides(fill = guide_colorbar(reverse = TRUE, barwidth = 7, barheight = 0.5, title.position = 'top')) + theme(legend.direction = "horizontal")
