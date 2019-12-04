
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
library(ggpubr)
library(cowplot)

source("C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\intensification\\rcode\\graphics_map_for_panels_intensification.R")
source("C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\intensification\\rcode\\graphics_dummy_legend.R")

drv <- dbDriver("PostgreSQL")

# con <- dbConnect(drv, dbname = "synthesis",
#                  host = "144.92.235.105", port = 5432,
#                  user = "mbougie", password = "Mend0ta!")



json_file <- "C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\json\\json_panels.json"
jsondata <- fromJSON(file=json_file)



# createDFlist <- function(col_list){
#   df_list = list()
#   for(i in col_list){
#     query = paste("SELECT * FROM synthesis_intensification.rfs_intensification_results_national WHERE column_name IN ('",i,"')", sep='')
#     print(query)
#     national_df <- dbGetQuery(con, query)
#     print(national_df)
#     df_list <- append(df_list, list(national_df))
#     
#   }
#   return(df_list)
# }
# 
# 
# 
# getggplotObject <- function(df_list, arg_list){
# 
#   ###declare the empty list that will hold all the ggplot objects
#   ggplot_object_list = list()
#   
#   for(j in 1:length(df_list)){
#     df = df_list[[j]]
#     print(df)
# 
#     index = as.numeric(df['index'])
#     print(typeof(index))
#     filename = as.character(as.vector(unname(df['column_name'])))
#     #filename = df$columnname
#     print('filename')
#     print(filename)
#   
# 
#     legendtitle = data$intensification$map[[filename]]$legend_title
#     print('-----------------legendtitle------------------')
#     print(legendtitle)
#     bin_breaks = data$intensification$map[[filename]]$bin_breaks
#     print(bin_breaks)
#     legend_labels = data$intensification$map[[filename]]$legend_labels
#     print(legend_labels)
#     legend_range = data$intensification$map[[filename]]$legend_range
#     print(legend_range)
#     title = data$intensification$map[[filename]]$title
#     print(title)
#     legend_position = data$intensification$map[[filename]]$legend_position
#     print(legend_position)
#     title_position_hjust = data$intensification$map[[filename]]$title_position_hjust
#     print(title_position_hjust)
#     legend_label_vjust = data$intensification$map[[filename]]$legend_label_vjust
#     print(legend_label_vjust)
#     legend_label_hjust = data$intensification$map[[filename]]$legend_label_hjust
#     print(legend_label_hjust)
# 
# 
#     ggplot_object = createMap(con, filename, legendtitle, bin_breaks, legend_labels, legend_range, title, legend_position,  title_position_hjust, legend_label_vjust,legend_label_hjust)
#     
#     
#     ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
#     
#   }
#   
#   return(ggplot_object_list)
# }  
 
getggplotObject <- function(filename_vector, arg_list){
  
  ###declare the empty list that will hold all the ggplot objects
  ggplot_object_list = list()
  
  for (filename in filename_vector){
    
    ggplot_object = createMap(con, jsondata, filename)
    
    
    ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
    
  }
  
  return(ggplot_object_list)
}  




# ############################################################################################
# # # panel 1 ################################################################################
# ############################################################################################
# ##create list of ggplots###################
# list_acres <- getggplotObject(c('acres_change_rot_cc', 'acres_change_rot_oo', 'acres_change_rot_co'))
# list_awa <- getggplotObject(c('acres_change_rot_cc_awa', 'acres_change_rot_oo_awa', 'acres_change_rot_co_awa'))
list_nleach <- getggplotObject(c('kg_change_rot_cc_nleach', 'kg_change_rot_oo_nleach', 'kg_change_rot_co_nleach', 'kg_change_total_nleach'))
# 
# ####create panel image ######################
dir = "D:\\projects\\synthesis\\s35\\intensification\\documents\\graphics\\"
fileout=paste(dir,"slide_1",".png", sep="")
# 
# legend1 <- get_legend(createDummy('acres') + theme(legend.position="bottom", legend.justification="center"))
# legend2 <- get_legend(createDummy('awa') + theme(legend.position="bottom", legend.justification="center"))

legend1 <- get_legend(createDummy('nleach') + theme(legend.position="bottom", legend.justification="center"))

col1 = plot_grid(plotlist = list_nleach, ncol = 1, nrow = 4, align = 'vh')
# col2 = plot_grid(plotlist = list_awa, ncol = 1, nrow = 3, align = 'vh')

plot_grid(col1, legend1, ncol = 1, rel_heights = c(1, .1))
ggsave(fileout, width = 20, height = 30, dpi = 800)



#### panel 2 #####################################################################################
list_pyield <- getggplotObject(c('kg_change_rot_cc_pyield', 'kg_change_rot_oo_pyield', 'kg_change_rot_co_pyield', 'kg_change_total_pyield'))
list_sedyield <- getggplotObject(c('ton_change_rot_cc_sedyield', 'ton_change_rot_oo_sedyield', 'ton_change_rot_co_sedyield', 'ton_change_total_sedyield'))
# list_napp <- getggplotObject(c('hectares_change_total_napp'))
# list_n2o <- getggplotObject(c('tons_co2e_change_total_n2o'))

####create panel image ######################
dir = "D:\\projects\\synthesis\\s35\\intensification\\documents\\graphics\\"
fileout=paste(dir,"slide_2",".png", sep="")

legend1 <- get_legend(createDummy('pyield') + theme(legend.position="bottom", legend.justification="center"))
legend2 <- get_legend(createDummy('sedyield') + theme(legend.position="bottom", legend.justification="center"))


col1 = plot_grid(plotlist = list_pyield, ncol = 1, nrow = 4, align = 'vh')
col2 = plot_grid(plotlist = list_sedyield, ncol = 1, nrow = 4, align = 'vh')


plot_grid(col1, col2, legend1, legend2, ncol = 2, rel_heights = c(1, .1))
ggsave(fileout, width = 23, height = 34, dpi = 800)














