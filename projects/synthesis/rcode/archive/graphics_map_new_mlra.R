
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




# drv <- dbDriver("PostgreSQL")
# 
# con <- dbConnect(drv, dbname = "usxp_deliverables",
#                  host = "144.92.235.105", port = 5432,
#                  user = "mbougie", password = "Mend0ta!")


# createMap <- function(con, filename, legend_title, bin_breaks, legend_labels, legend_range, title, legend_position, title_position_hjust, legend_label_vjust, legend_label_hjust){
createMap <- function(con, filename, json_data){

  
  
  

legend_title = json_data$extensification$map[[filename]]$legend_title
print(legend_title)
bin_breaks = json_data$extensification$map[[filename]]$bin_breaks
print(bin_breaks)
legend_labels = json_data$extensification$map[[filename]]$legend_labels
print(legend_labels)
legend_range = json_data$extensification$map[[filename]]$legend_range
print(legend_range)
title = json_data$extensification$map[[filename]]$title
print(title)
legend_position = json_data$extensification$map[[filename]]$legend_position
print(legend_position)
title_position_hjust = json_data$extensification$map[[filename]]$title_position_hjust
print(title_position_hjust)
legend_label_vjust = json_data$extensification$map[[filename]]$legend_label_vjust
print(legend_label_vjust)
legend_label_hjust = json_data$extensification$map[[filename]]$legend_label_hjust
print(legend_label_hjust)  
  
  
  
me <-function(){"blue"}
##########################  using supplied county dataset######################################   

#### get the built in state feature layer
state_init <- map_data("state")
state <- readOGR(dsn = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\sf", layer = "states_wgs84")
state.df <- fortify(state)
state.df <- state.df[order(state.df$order),]

summary(state.df)
summary(state_init)

# #### create counties dataset from buit in dataset
# cnty_intial <- map_data("county")

# #### get the fips code
# data(county.fips)
# 
# #### attach fips code to counties dataset
# cnty <- cnty_intial %>%
#   mutate(polyname = paste(region,subregion,sep=",")) %>%
#   left_join(county.fips, by="polyname")


# filename = 'hectares_change_rot_co_napp'
## query the data from postgreSQL 
postgres_query <- dbGetQuery(con, gsub("current_field", filename, "SELECT lrr_group, current_field as percent FROM synthesis_extensification.extensification_mlra WHERE current_field IS NOT NULL;"))


mapa <- readOGR(dsn = "D:\\projects\\usxp\\deliverables\\maps\\synthesis\\extensification\\shapefiles", layer = "extensification_mlra_wgs84")
# mapa
mapa@data$id <- rownames(mapa@data)
mapa.df     <- fortify(mapa)
mapa.df <- join(mapa.df, mapa@data, by="id")
mapa.df <- merge(mapa.df, postgres_query, by.x="lrr_group", by.y="lrr_group")
mapa.df <- mapa.df[order(mapa.df$order),]
mapa.df$test = cut(mapa.df$percent, breaks= bin_breaks)


d = ggplot() +
  scale_fill_manual(values = rev(brewer.pal(10, 'PRGn')[legend_range]),
                    labels = legend_labels,
                    guide = guide_legend( 
                                          title=legend_title,
                                          title.theme = element_text(
                                                                    size = 27,
                                                                    # face = "italic",
                                                                    color = "#4e4d47",
                                                                    vjust=0.0,
                                                                    angle = 0
                                                                   ),
                                           # these are the legend bin dimnesions
                                           keyheight = unit(3, units = "mm"),

                                           keywidth = unit(20, units = "mm"),
                                           # keywidth = unit(20, units = "mm"),
                                           label.position = "bottom",
                                           title.position = 'top',
                                           nrow=1
                                           ####these control the placement of the numeric values labels for the legend
                                           # label.vjust=legend_label_vjust,
                                           # # ncol=10,
                                           # label.hjust=legend_label_hjust

                                        )
                     ) +


  ### state grey background ###########
   geom_polygon(
    data=state.df,
    aes(y=lat, x=long, group=group),
    fill='#cccccc'
  )+
# 
  ## county choropleth map ###########
  geom_polygon(
    data=mapa.df,
    aes(y=lat, x=long, group=group, fill = test),
    colour = '#cccccc',
    size = 0.5
  ) +
# 
  ### state boundary strokes ###########
  geom_polygon(
    data=state.df,
    aes(y=lat, x=long, group=group),
    alpha=0,
    colour='white',
    size=0.5
  )+
  
  coord_map(project="polyconic") +
  labs(
    title = title
    # caption = "increase: 15 kg
    #            decrease: 12 kg
    #                 net: 3 kg"

    # caption = caption(expression(phantom("title (") * "slope=1"), col.main = "red")
    # subtitle = "For cumulative year 2015"
  ) +
# 
# 
# 
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
    # legend.position="none"
    legend.position = legend_position
  )

print(d)
return(d)
} 


# json_file <- "C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\deliverables\\synthesis\\json\\json_panels.json"
# json_data <- fromJSON(file=json_file)
# print('json_data')
# print(json_data)
# 
# 
# 
# filename = 'perc_expand_rfs'
# print('filename')
# print(filename)
# 
# 
# createMap(con, filename, json_data)
