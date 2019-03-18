
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
# library(viridis)
library(scales)
require(RColorBrewer)
library(glue)
library(gridBase)
library(grid)
library(gridExtra) #load Grid



createMap <- function(con, filename, legend_title, bin_breaks, legend_labels, legend_range, title, legend_position, title_position_hjust, legend_label_vjust, legend_label_hjust){

me <-function(){"blue"}
##########################  using supplied county dataset######################################   

#### get the built in state feature layer
state <- map_data("state") 

#### create counties dataset from buit in dataset
cnty_intial <- map_data("county")

#### get the fips code
data(county.fips)

#### attach fips code to counties dataset
cnty <- cnty_intial %>%
  mutate(polyname = paste(region,subregion,sep=",")) %>%
  left_join(county.fips, by="polyname")


## query the data from postgreSQL 
postgres_query <- dbGetQuery(con, gsub("current_field", filename, "SELECT atlas_stco, current_field as percent FROM synthesis_intensification.rfs_intensification_results_counties WHERE current_field IS NOT NULL;"))

## merge cnty2 with postgres_query
d = merge(cnty, postgres_query, sort = TRUE, by.x='fips', by.y='atlas_stco')

## If I don't reorder the order of lat long it "tears" the polygons!
cnty_fnl<-d[order(d$order),]


# cnty_fnl$test = cut(cnty_fnl$percent, breaks= c(-Inf, -5000, 0, 5000, 10000, 20000, 40000, 80000))
cnty_fnl$test = cut(cnty_fnl$percent, breaks= bin_breaks)

cnty_fnl$test




d = ggplot() + 
  scale_fill_manual(values = rev(brewer.pal(10, 'BrBG')[legend_range]),
                    labels = legend_labels,
                    guide = guide_legend( title=legend_title,
                                          title.theme = element_text(
                                                                    size = 27,
                                                                    # face = "italic",
                                                                    color = "#4e4d47",
                                                                    vjust=0.0,
                                                                    angle = 0
                                                                   ), 
                                           # these are the legend bin dimnesions
                                           keyheight = unit(3, units = "mm"),
                           
                                           keywidth = unit(5, units = "mm"),
                                           # keywidth = unit(20, units = "mm"), 
                                           label.position = "bottom", 
                                           title.position = 'top',
                                           nrow=1,
                                           ####these control the placement of the numeric values labels for the legend
                                           label.vjust=legend_label_vjust,
                                           # ncol=10,
                                           label.hjust=legend_label_hjust
                                  
                                        ) 
                     ) +
   
  
  ### state grey background ###########
   geom_polygon(
    data=state,
    aes(y=lat, x=long, group=group),
    fill='#cccccc'
  )+
   
  ### county choropleth map ###########
  geom_polygon(
    data=cnty_fnl,
    # fill='grey70',
    aes(y=lat, x=long, group=group, fill = test),
    colour = '#cccccc',
    size = 0.5
  ) +
  
  ### state boundary strokes ###########
  geom_polygon(
    data=state,
    aes(y=lat, x=long, group=group),
    # fill='grey70',
    alpha=0,
    colour='white',
    size=0.5
  )+
  
  # geom_polygon(data = states.df, aes(x = long, y = lat, group = group), fill = '#cccccc', colour = NA) +
  coord_map(project="polyconic") +
  # labs(
  #   title = title,
  #   caption = "increase: 15 kg
  #              decrease: 12 kg
  #                   net: 3 kg"
  # 
  #   # caption = caption(expression(phantom("title (") * "slope=1"), col.main = "red")
  #   # subtitle = "For cumulative year 2015"
  # ) +
  
 
  
  theme(
    text = element_text(color = "#4e4d47", size=30),   ##these are the legend numeric values
    axis.text.x = element_blank(),
    axis.title.x=element_blank(),
    axis.text.y = element_blank(),
    axis.title.y=element_blank(),
    axis.ticks = element_blank(),
    panel.grid.major = element_blank(),
    plot.background = element_rect(fill = NA, color = NA),
    plot.margin = unit(c(0, 0, 0, 0), "cm"),
    panel.background = element_rect(fill = NA, color = NA),
    # legend.background = element_rect(fill = "white", color = NA),
    plot.title = element_text(size= 35, vjust=0.5, hjust=title_position_hjust, color = "#4e4d47"),
    plot.caption = element_text(size= 18, color = "blue"),
    # plot.subtitle = element_text(size= 10,vjust=0.5, hjust=0.55, color = "#4e4d47", margin = margin(b = -0.1, t = 0.20, l = 2, unit = "cm")),
    ###controls the lgend palcement (graphics AND labels)
    legend.position="none"
    # legend.position = legend_position
  )

 
return(d)
} 

