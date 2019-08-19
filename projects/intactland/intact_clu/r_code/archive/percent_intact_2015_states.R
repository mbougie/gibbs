
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "intact_lands",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")



##########################  using supplied county dataset######################################   
state <- map_data("state") 
##subset state dataset
# state_ss <- subset(state, region=='montana' | region=='wyoming' | region == 'north dakota'| region=='south dakota' | region == 'minnesota' | region=='iowa' | region == 'nebraska')
state_ss <- subset(state, region=='montana' | region=='wyoming' | region == 'north dakota'| region=='south dakota' | region == 'minnesota' | region=='iowa' | region == 'nebraska')


## query the data from postgreSQL 
yo <- dbGetQuery(con, "SELECT 
                        lower(b.atlas_name) as region,
                        sum(a.acres_2015_c),
                        round(((sum(a.acres_2015_c)/b.acres_calc)::numeric)*100, 1) as percent
                      FROM 
                        clu_2015.clu_2015_noncrop_c as a JOIN spatial.states as b ON LEFT(atlas_stco,2) = b.atlas_st
                      GROUP BY b.atlas_name,b.acres_calc;")

## merge cnty2 with yo
d = merge(state, yo, sort = TRUE, by='region')

## If I don't reorder the order of lat long it "tears" the polygons!
state_fnl<-d[order(d$order),]


cnames <- aggregate(cbind(long, lat) ~ percent, data=state_fnl, 
                    FUN=function(x)mean(range(x)))


# 
# d = (ggplot() +
#   geom_polygon(data=state_fnl, aes(y=lat, x=long, group=group, fill = percent), colour = 'grey50', size = 0.25) +
#   
#   ### this is a hack to add the label to the center---add a new layer of centroids and label them 
#   geom_point(colour='red',  fill = 'grey70', alpha=0) +
#   geom_text(data=cnames, aes(long, lat, label = percent), size=3, color='white') +
#   
#   coord_map(project="polyconic") +
#   theme(plot.title = element_text(colour = "steelblue",  face = "bold", family = "Helvetica", hjust = 0.5),
#         axis.text.x = element_blank(),
#         axis.title.x=element_blank(),
#         axis.text.y = element_blank(),
#         axis.title.y=element_blank(),
#         axis.ticks = element_blank(),
#         panel.grid.major = element_blank()))
# 
# d +  scale_fill_gradient(low = "#9ebcda", high = "#4d004b", name='percent') + 
#   ggtitle("Percent Intact Lands by State (2015)")
# 


d = ggplot() +
  geom_polygon(data=state_fnl, aes(y=lat, x=long, group=group, fill = percent), colour = 'grey50', size = 0.25) + 
  
  
  scale_fill_continuous(name="percent", trans = "reverse") +
  
  geom_polygon(data=state_ss, aes(y=lat, x=long, group=group), fill = 'grey70', alpha=0, colour = 'white', size = 0.9)+
  
  
  ### this is a hack to add the label to the center---add a new layer of centroids and label them
  geom_point(colour='red',  fill = 'grey70', alpha=0) +
  geom_text(data=cnames, aes(long, lat, label = percent), size=4, color='white') +

  coord_map(project="polyconic") +

  labs(
    title = "Percent Potential Undisturbed Lands by State",
    subtitle = "For cumulative year 2015"
  ) +
  
  theme(
    text = element_text(color = "#22211d"), 
    axis.text.x = element_blank(),
    axis.title.x=element_blank(),
    axis.text.y = element_blank(),
    axis.title.y=element_blank(),
    axis.ticks = element_blank(),
    panel.grid.major = element_blank(),
    plot.background = element_rect(fill = "#f5f5f2", color = NA), 
    panel.background = element_rect(fill = "#f5f5f2", color = NA), 
    legend.background = element_rect(fill = "#f5f5f2", color = NA),
    
    
    plot.title = element_text(size= 15, hjust=0.00, color = "#4e4d47", margin = margin(b = -0.1, t = 0.1, l = 0.01, unit = "cm")),
    plot.subtitle = element_text(size= 10, hjust=0.00, color = "#4e4d47", margin = margin(b = -0.2, t = 0.20, l = 0.01, unit = "cm")),
    legend.position = c(0.08, 0.05)
    
    
  )

d + guides(fill = guide_colorbar(reverse = TRUE, barwidth = 7, barheight = 0.5, title.position = 'top')) + theme(legend.direction = "horizontal") 




