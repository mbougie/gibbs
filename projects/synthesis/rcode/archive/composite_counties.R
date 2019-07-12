
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "usxp_deliverables",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")

##########################  using supplied county dataset######################################   
state <- map_data("state") 
##subset state dataset
state_ss <- subset(state, region=='montana' | region=='wyoming' | region == 'north dakota'| region=='south dakota' | region == 'minnesota' | region=='iowa' | region == 'nebraska')

##create counties dataset
cnty <- map_data("county")

##get the fips code
data(county.fips)

## attach fips code to counties dataset
cnty2 <- cnty %>%
  mutate(polyname = paste(region,subregion,sep=",")) %>%
  left_join(county.fips, by="polyname")

## query the data from postgreSQL 
yo <- dbGetQuery(con, "SELECT atlas_stco, acres_change_rot_cc as count FROM synthesis_intensification.rfs_intensification_results_counties;")

## merge cnty2 with yo
d = merge(cnty2, yo, sort = TRUE, by.x='fips', by.y='atlas_stco')

## If I don't reorder the order of lat long it "tears" the polygons!
cnty_fnl<-d[order(d$order),]


# d = (ggplot() +
#   geom_polygon(data=cnty_fnl, aes(y=lat, x=long, group=factor(group), fill = factor(count)), colour = 'grey50', size = 0.25) + 
#   geom_polygon(data=state_ss, aes(y=lat, x=long, group=group), fill = 'grey70', alpha=0, colour = 'white', size = 0.25)+
#   coord_map(project="polyconic") +
#   theme(plot.title = element_text(colour = "steelblue",  face = "bold", family = "Helvetica", hjust = 0.5),
#         axis.text.x = element_blank(),
#         axis.title.x=element_blank(),
#         axis.text.y = element_blank(),
#         axis.title.y=element_blank(),
#         axis.ticks = element_blank(),
#         panel.grid.major = element_blank()))
# 
# d + scale_fill_brewer(palette = "PuOr", name = "counts")+
#     ggtitle("Datasets per County")
# 


d = ggplot() +
  geom_polygon(data=cnty_fnl, aes(y=lat, x=long, group=factor(group), fill = factor(count)), colour = 'grey50', size = 0.25) + 
  geom_polygon(data=state_ss, aes(y=lat, x=long, group=group), fill = 'grey70', alpha=0, colour = 'white', size = 0.9)+
  
  
  # scale_fill_brewer(breaks=c(-80000,-40000,-20000,-10000,0,10000,20000), name="CLU count", guide = guide_legend( keyheight = unit(2, units = "mm"), keywidth=unit(5, units = "mm"), label.position = "bottom", title.position = 'top', nrow=1) ) +
  scale_fill_distiller(name="Percent", palette = "BrBG", breaks = pretty_breaks(n = 8))+
  
  # scale_fill_gradient2(midpoint = 3, name='percent')+
  geom_polygon(data=state_ss, aes(y=lat, x=long, group=group), fill = 'grey70', alpha=0, colour = 'white', size = 0.25)+
  coord_map(project="polyconic") +
  # theme(plot.title = element_text(colour = "steelblue",  face = "bold", family = "Helvetica", hjust = 0.5),
  #       axis.text.x = element_blank(),
  #       axis.title.x=element_blank(),
  #       axis.text.y = element_blank(),
  #       axis.title.y=element_blank(),
  #       axis.ticks = element_blank(),
  #       panel.grid.major = element_blank())+
  labs(
    title = "Number of CLUs per County",
    subtitle = "From years 2003 to 2015"
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
    
    plot.title = element_text(size= 15, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.1, l = 0.01, unit = "cm")),
    plot.subtitle = element_text(size= 10, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.20, l = 2, unit = "cm")),
    legend.position = c(0.12, 0.06)
    
    
  )

d





