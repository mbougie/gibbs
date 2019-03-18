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



createDummy <- function(filename){
  
  json_file <- "C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\deliverables\\synthesis\\json\\json_legends.json"
  data <- fromJSON(file=json_file)
  
  breaks = data$intensification$map[[filename]]$bin_breaks
  legend_labels = data$intensification$map[[filename]]$legend_labels
  legend_title = data$intensification$map[[filename]]$legend_title
  legend_label_vjust = data$intensification$map[[filename]]$legend_label_vjust
  legend_label_hjust = data$intensification$map[[filename]]$legend_label_hjust
  breaks
  print(breaks)
  

  onePrior = function(x){
    sign = x/abs(x)
    if(is.nan(sign)){
      return(x)
    }else{
      return(sign*(abs(x)-1))
    }
  }
  
  vec = sapply(breaks, onePrior)
  
  df = data.frame(cbind(x = vec, y = vec))
  
  df['cuts'] = cut(df$x, breaks= breaks)
  df = df[which(df$x != 0),]  # JANKALERT
  
  d = ggplot(df, aes(x = x, y = y, fill = cuts)) +
    geom_area() +
    scale_fill_manual(values = rev(brewer.pal(10, 'BrBG')),
                      labels = legend_labels,
                      guide = guide_legend( title=legend_title,
                                            title.theme = element_text(
                                              size = 27,
                                              color = "#4e4d47",
                                              vjust=0.0,
                                              angle = 0
                                            ),
                                            label.theme = element_text(
                                              size = 20,
                                              color = "#4e4d47",
                                              vjust=0.0,
                                              angle = 0
                                            ),
                                            # these are the legend bin dimnesions
                                            keyheight = unit(3, units = "mm"),
                                            keywidth = unit(17, units = "mm"),
                                            label.position = "bottom",
                                            title.position = 'top',
                                            nrow=1
                                            ##these control the placement of the numeric values labels for the legend
                                            # label.vjust=legend_label_vjust,
                                            # label.hjust=legend_label_hjust
                                            
                                           )
                    )
  
  return(d)
}

# createDummy('awa')

