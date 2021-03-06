
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
# library(dplyr)
library(viridis)
library(scales)
library(rjson)
# library(jsonlite)
require(RColorBrewer)
library(glue)
# library(ggpubr)
library(cowplot)

rootpath = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\deliverables\\choropleths'

#####link to the other two scripts
source(paste(rootpath, 'rcode\\choropleths_maps.R', sep='\\'))
# source(paste(rootpath, 'json\\graphics_dummy_legend.R', sep='\\'))

json_file = paste(rootpath, 'json\\json_panels.json', sep='\\')
jsondata <- fromJSON(file=json_file)




###########################################################################################
#####get the dataframes###################################################################
###########################################################################################
user <- "mbougie"
host <- '144.92.235.105'
port <- '5432'
password <- 'Mend0ta!'


con_nass <- dbConnect(PostgreSQL(), dbname = 'nass', user = user, host = host, port=port, password = password)
nass <- get_postgis_query(con_nass, "SELECT atlas_stco, perc_conv_county as perc, geom FROM ag_census.ag_census_expansion WHERE perc_conv_county > 0.5",
                          geom_name = "geom")
###attach df to specific object in json
jsondata$nass$df <- nass


con_nri <- dbConnect(PostgreSQL(), dbname = 'nri', user = user, host = host, port=port, password = password)
nri <- get_postgis_query(con_nri, "SELECT atlas_stco, perc_expansion as perc, geom FROM main.nri INNER JOIN spatial.counties USING(atlas_stco) WHERE perc_expansion > 0.5",
                          geom_name = "geom")
###attach df to specific object in json
jsondata$nri$df <- nri



con_usxp_deliverables <- dbConnect(PostgreSQL(), dbname = 'usxp_deliverables', user = user, host = host, port=port, password = password)
nlcd <- get_postgis_query(con_usxp_deliverables, "SELECT
                                                     combine_nlcd08_16_histo.atlas_stco,
                                                     (combine_nlcd08_16_histo.acres/counties.acres_calc)*100 as perc,
                                                     geom
                                                   FROM
                                                     choropleths.combine_nlcd08_16_histo INNER JOIN
                                                     spatial.counties
                                                   ON
                                                     counties.atlas_stco = combine_nlcd08_16_histo.atlas_stco
                                                   WHERE label = '4' AND (combine_nlcd08_16_histo.acres/counties.acres_calc)*100 > 0.5",
                                                  geom_name = "geom")

###attach df to specific object in json
jsondata$nlcd$df <- nlcd


usxp <- get_postgis_query(con_usxp_deliverables, "SELECT atlas_stco, perc_conv_county as perc, geom FROM choropleths.s35_perc_conv_county WHERE perc_conv_county > 0.5",
                          geom_name = "geom")

###attach df to specific object in json
jsondata$usxp$df <- usxp




######################################################################################################
############  get ggplot objects #####################################################################
######################################################################################################

getggplotObject <- function(obj_vector){
  
  ###declare the empty list that will hold all the ggplot objects
  ggplot_object_list <- list()
  
  for (obj in obj_vector){
    
    
    ggplot_object = createMap(obj)
    
    ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
    
  }
  
  return(ggplot_object_list)
}  
 



############################################################################################
# # panel 1 ################################################################################
###########################################################################################


#### store dataframes in here!?
list_exp <- getggplotObject(list(jsondata$usxp, jsondata$nri))
list_aban <- getggplotObject(list(jsondata$nlcd, jsondata$nass))

###create panel image ######################
dir = "I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\choropleths\\deliverables\\"
fileout=paste(dir,"test",".png", sep="")

# legend1 <- get_legend(createDummy('acres') + theme(legend.position="bottom", legend.justification="center"))
# legend2 <- get_legend(createDummy('awa') + theme(legend.position="bottom", legend.justification="center"))

col1 = plot_grid(plotlist = list_exp, ncol = 1, nrow = 2, align = 'vh')
col2 = plot_grid(plotlist = list_aban, ncol = 1, nrow = 2, align = 'vh')

# plot_grid(col1,col2, ncol = 2, rel_heights = c(1, .1))
plot_grid(col1,col2, ncol = 2)
ggsave(fileout, width = 34, height = 25, dpi = 500)




