
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

source("C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\extensification\\rcode\\graphics_map_for_panels_extensification.R")
source("C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\extensification\\rcode\\graphics_dummy_legend.R")

drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "usxp_deliverables",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")


con2 <- dbConnect(drv, dbname = "synthesis",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")



json_file <- "C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\json\\json_panels.json"
jsondata <- fromJSON(file=json_file)



getggplotObject <- function(filename_vector, arg_list){

  ###declare the empty list that will hold all the ggplot objects
  ggplot_object_list = list()
  
  for (filename in filename_vector){
  
    ggplot_object = createMap(con, con2, jsondata, filename)
    
    
    ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
    
  }
  
  return(ggplot_object_list)
}  
 



############################################################################################
# # panel 1 ################################################################################
###########################################################################################
# ##create list of ggplots###################
# list_exp <- getggplotObject(c('ratio_expand_rfs_mlra', 'e_gigagrams_co2e', 'gal_et_exp_imp_rfs'))
# list_aban <- getggplotObject(c('ratio_abandon_rfs_mlra', 's_gigagrams_co2e', 'gal_et_aban_imp_rfs'))

list_exp <- getggplotObject(c('p_exp_imp_rfs', 'n_exp_imp_rfs', 'sed_exp_imp_rfs'))
list_aban <- getggplotObject(c('p_aban_imp_rfs', 'n_aban_imp_rfs', 'sed_aban_imp_rfs'))



###create panel image ######################
dir = "C:\\Users\\Bougie\\Box\\data_science_study_group\\resources\\demos\\matt_bougie\\graphics\\"
fileout=paste(dir,"extensification_panel_1",".png", sep="")

# legend1 <- get_legend(createDummy('acres') + theme(legend.position="bottom", legend.justification="center"))
# legend2 <- get_legend(createDummy('awa') + theme(legend.position="bottom", legend.justification="center"))

col1 = plot_grid(plotlist = list_exp, ncol = 1, nrow = 3, align = 'vh')
col2 = plot_grid(plotlist = list_aban, ncol = 1, nrow = 3, align = 'vh')

plot_grid(col1, col2,ncol = 2, rel_heights = c(1, .1))
ggsave(fileout, width = 23, height = 34, dpi = 800)



##### panel 2 #####################################################################################
# list_exp <- getggplotObject(list('perc_expand_rfs'))
# list_aban <- getggplotObject(c('perc_abandon_rfs'))
# 
# ####create panel image ######################
# dir = "D:/projects/usxp/deliverables/maps/synthesis/extensification/documents/graphics/"
# fileout=paste(dir,"extensification_panel_2",".png", sep="")
# 
# col1 = plot_grid(plotlist = list_exp, ncol = 1, nrow = 1, align = 'vh')
# col2 = plot_grid(plotlist = list_aban, ncol = 1, nrow = 1, align = 'vh')
# 
# plot_grid(col1, col2,ncol = 2, rel_heights = c(1, .1))
# ggsave(fileout, width = 25, height = 8.33, dpi = 800)





