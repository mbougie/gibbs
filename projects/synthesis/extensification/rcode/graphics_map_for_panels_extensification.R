
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



createMap <- function(con, con2, jsondata, filename, current_ds){

  
###create global variables
  
legend_title = jsondata$extensification$map[[filename]]$legend_title
print('-----------------legendtitle------------------')
print(legend_title)
bin_breaks = jsondata$extensification$map[[filename]]$bin_breaks
print(bin_breaks)
legend_labels = jsondata$extensification$map[[filename]]$legend_labels
print(legend_labels)
legend_range = jsondata$extensification$map[[filename]]$legend_range
print(legend_range)
title = jsondata$extensification$map[[filename]]$title
print(title)
legend_position = jsondata$extensification$map[[filename]]$legend_position
print(legend_position)
title_position_hjust = jsondata$extensification$map[[filename]]$title_position_hjust
print(title_position_hjust)
legend_label_vjust = jsondata$extensification$map[[filename]]$legend_label_vjust
print(legend_label_vjust)
legend_label_hjust = jsondata$extensification$map[[filename]]$legend_label_hjust
print(legend_label_hjust)
dataset = jsondata$extensification$map[[filename]]$dataset
print(dataset)  
  
  
  
  
##########################  using supplied county dataset######################################   

#### get the built in state feature layer
state_init <- map_data("state")
state <- readOGR(dsn = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\sf", layer = "states_wgs84")
state.df <- fortify(state)
state.df <- state.df[order(state.df$order),]

summary(state.df)
summary(state_init)


getQuery <- function(){
  if(dataset == "acres"){
    return(dbGetQuery(con, gsub("current_field", filename, "SELECT lrr_group, current_field as percent FROM synthesis_extensification.extensification_mlra WHERE current_field IS NOT NULL;")))}
  
  else if (dataset == "agroibis"){
    return(dbGetQuery(con2, gsub("current_field", filename, "SELECT fips, current_field as percent FROM extensification_agroibis.agroibis_counties WHERE current_field != 0;")))}
  
  else if (dataset == "carbon"){
    return(dbGetQuery(con, gsub("current_field", filename, "SELECT atlas_stco, current_field as percent FROM synthesis_extensification.carbon_rfs_counties_v2 WHERE current_field IS NOT NULL;")))}
  }




## query the data from postgreSQL 
# postgres_query <- dbGetQuery(con, gsub("current_field", filename, "SELECT lrr_group, current_field as percent FROM synthesis_extensification.extensification_mlra WHERE current_field IS NOT NULL;"))
postgres_query <- getQuery()

getMapDF <- function(){
  if(dataset == "acres"){
    current <- readOGR(dsn = "D:\\projects\\synthesis\\s35\\extensification\\shapefiles", layer = "extensification_mlra_wgs84")
    # current
    current@data$id <- rownames(current@data)
    current.df     <- fortify(current)
    current.df <- join(current.df, current@data, by="id")
    current.df <- merge(current.df, postgres_query, by.x="lrr_group", by.y="lrr_group")
    current.df <- current.df[order(current.df$order),] 
  }
  else if (dataset == "agroibis"){
    # current <- readOGR(dsn = "D:\\projects\\synthesis\\s35\\extensification\\shapefiles", layer = "agroibis_counties")
    current <- readOGR(dsn='D:\\projects\\synthesis\\s35\\extensification\\extensification.gdb',layer="agroibis_counties")
    # current
    current@data$id <- rownames(current@data)
    current.df     <- fortify(current)
    current.df <- join(current.df, current@data, by="id")
    current.df <- merge(current.df, postgres_query, by.x="fips", by.y="fips")
    current.df <- current.df[order(current.df$order),]
  }
  else if (dataset == "carbon"){
    current <- readOGR(dsn = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\sf", layer = "counties_wgs84")
    # current
    current@data$id <- rownames(current@data)
    current.df     <- fortify(current)
    current.df <- join(current.df, current@data, by="id")
    current.df <- merge(current.df, postgres_query, by.x="atlas_stco", by.y="atlas_stco")
    current.df <- current.df[order(current.df$order),]
  }
}
  
  
mapa.df <- getMapDF()






getFillValues <- function(filename, legend_range){
  print(filename)
  v <- c('p_aban_imp_rfs', 'n_aban_imp_rfs', 'sed_aban_imp_rfs')
  # v <- c( 'et_aban_imp_rfs', 'carbon_sequester')
 
  print(match(filename,v))
  if(filename %in% v){
    print('--------match-----------------')
    mapa.df$percent <- mapa.df$percent * -1
    print(mapa.df$percent)
    return(cut(mapa.df$percent, breaks= bin_breaks))
  }
  else{
    print('-------NO-match-----------------')
    print(cut(mapa.df$percent, breaks= bin_breaks))
    return(cut(mapa.df$percent, breaks= bin_breaks))
    }
}
  
 
mapa.df$fill = getFillValues(filename, legend_range)  


# filename = 'perc_abandon_rfs'
getRange <- function(filename, legend_range){
  # rthis is to fill the values for some of the maps
  v <- c('perc_abandon_rfs')
  print('--------match-----------------')
  print(match(filename,v))
  if(filename %in% v){
    return(brewer.pal(10, 'PRGn')[legend_range])
  }
  else{return(rev(brewer.pal(10, 'PRGn')[legend_range]))}
}

getColorVector <- function(){
  v <- c('gal_irr_exp_imp_rfs', 'gal_irr_aban_imp_rfs')

  if(filename %in% v){
    color_vector = (brewer.pal(10, 'PRGn'))
    print(color_vector) 
    color_vector[6] = '#f7f7f7'
    print(color_vector) 
    return(color_vector)
  }
  else{return(brewer.pal(10, 'PRGn')[legend_range])}
}

d = ggplot() + 
  # scale_fill_manual(values = rev(brewer.pal(10, 'PRGn')[legend_range]),
  scale_fill_manual(values = rev(getColorVector()),
                    labels = legend_labels,
                    # limits = c(min(bin_breaks), max(bin_breaks)),
                    guide = guide_legend( title=legend_title,
                                          title.theme = element_text(
                                                                    size = 27,
                                                                    # size = 16,
                                                                    # face = "italic",
                                                                    color = "#4e4d47",
                                                                    vjust=0.0,
                                                                    angle = 0
                                                                   ), 
                                           # these are the legend bin dimnesions
                                           keyheight = unit(3, units = "mm"),
                           
                                           # keywidth = unit(5, units = "mm"),
                                           keywidth = unit(20, units = "mm"), 
                                           label.position = "bottom", 
                                           title.position = 'top',
                                           nrow=1
                                           ####these control the placement of the numeric values labels for the legend
                                           # label.vjust=legend_label_vjust,
                                           # ncol=10,
                                           # label.hjust=legend_label_hjust
                                  
                                        ) 
                     ) +
   
  
  ### state grey background ###########
   geom_polygon(
    data=state.df,
    aes(y=lat, x=long, group=group),
    fill='#cccccc'
  )+
   
  ### county choropleth map ###########
  geom_polygon(
    data=mapa.df,
    # fill='grey70',
    aes(y=lat, x=long, group=group, fill = fill),
    colour = '#cccccc',
    size = 0.5
  ) +
  
  ### state boundary strokes ###########
  geom_polygon(
    data=state.df,
    aes(y=lat, x=long, group=group),
    # fill='grey70',
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

 
  
  theme(
    text = element_text(color = "#4e4d47", size=30),   ##these are the legend numeric values
    axis.text.x = element_blank(),
    axis.title.x=element_blank(),
    axis.text.y = element_blank(),
    axis.title.y=element_blank(),
    axis.ticks = element_blank(),
    axis.line = element_blank(),
    panel.grid.major = element_blank(),
    plot.background = element_rect(fill = NA, color = NA),
    plot.margin = unit(c(0, 0, 0, 0), "cm"),
    panel.background = element_rect(fill = NA, color = NA),
    # panel.border = element_blank(),
    # legend.background = element_rect(fill = "white", color = NA),
    plot.title = element_text(size= 25, vjust=-12.0, hjust=title_position_hjust, color = "#4e4d47"),
    # plot.title = element_text(size= 16, vjust=0.5, hjust=title_position_hjust, color = "#4e4d47"),
    plot.caption = element_text(size= 18, color = "blue"),
    # plot.subtitle = element_text(size= 10,vjust=0.5, hjust=0.55, color = "#4e4d47", margin = margin(b = -0.1, t = 0.20, l = 2, unit = "cm")),
    ###controls the lgend palcement (graphics AND labels)
    # legend.position="none"
    legend.position = legend_position
  )


return(d)

} 




