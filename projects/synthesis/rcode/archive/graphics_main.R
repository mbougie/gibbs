
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

source("C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\deliverables\\synthesis\\rcode\\graphics_map.R")




drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "usxp_deliverables",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")




json_file <- "C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\deliverables\\synthesis\\json\\json_panels.json"
data <- fromJSON(file=json_file)
print('data')
print(data)

# query the data from postgreSQL
# national_df <- dbGetQuery(con, "SELECT * 
#                                 FROM synthesis_intensification.rfs_intensification_results_national 
#                                 WHERE columnname IN ('acres_change_rot_co_awa')")
# national_df
#  
# data$intensification

f = function() {

  # index = as.numeric(x['index'])
  # print(typeof(index))
  filename = 'hectares_change_rot_co_napp'
  print('filename')
  print(filename)

  

  
  legendtitle = data$intensification$map[[filename]]$legend_title
  print(legendtitle)
  bin_breaks = data$intensification$map[[filename]]$bin_breaks
  print(bin_breaks)
  legend_labels = data$intensification$map[[filename]]$legend_labels
  print(legend_labels)
  legend_range = data$intensification$map[[filename]]$legend_range
  print(legend_range)
  title = data$intensification$map[[filename]]$title
  print(title)
  legend_position = data$intensification$map[[filename]]$legend_position
  print(legend_position)
  title_position_hjust = data$intensification$map[[filename]]$title_position_hjust
  print(title_position_hjust)
  legend_label_vjust = data$intensification$map[[filename]]$legend_label_vjust
  print(legend_label_vjust)
  legend_label_hjust = data$intensification$map[[filename]]$legend_label_hjust
  print(legend_label_hjust)
  
 
  
  createMap(con, filename, legendtitle, bin_breaks, legend_labels, legend_range, title, legend_position,  title_position_hjust, legend_label_vjust,legend_label_hjust)


  dir = "C:/Users/Bougie/Desktop/temp/"


  fileout=paste(dir,filename,".png", sep="")


  ggsave(fileout, width = 18, height = 12, dpi = 800)
  
}

f()

# apply(national_df, 1, f)



