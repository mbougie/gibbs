####################################################################################
############### new legend ##########################################################
####################################################################################


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



drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "intactland",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")



###NOTE: ALWYS CHECK THIS!!!!! ###############################
million = 10^6
hundered_thousand = 10^5

formatAC<-function(x){x/million}  





##### query the data from postgreSQL
df <- dbGetQuery(con,"
                    -----------------------------------------------------
                    ----intact lands (ALL lands protected and non-protected)
                    -----------------------------------------------------
                    SELECT 
                    b.st_abbrev, 
                    a.acres::integer as acres,
                    c.label
                    -----------------------------------------------------
                    FROM 
                    intact_clu.intactland_15_refined_cdl15_broad_pad_hist_states as a 
                    
                    INNER JOIN
                    spatial.states as b
                    USING(atlas_st)
                    
                    INNER JOIN
                    intact_clu.intactland_15_refined_cdl15_broad_pad as c
                    ON a.value::integer = c.value
                    
                    
                    UNION
                    -------------------------------------------------------------------
                    ----NON-intact lands (ALL lands protected and non-protected)
                    -------------------------------------------------------------------
                    SELECT 
                    b.st_abbrev, 
                    sum(a.acres::integer) as acres,
                    c.label
                    
                    FROM 
                    intact_clu.combined_hist_states as a 
                    INNER JOIN
                    spatial.states as b
                    USING(atlas_st)
                    INNER JOIN
                    intact_clu.combined as c
                    ON a.value::integer = c.value
                    
                    ---don't want to work with intact lands
                    WHERE c.label not in ('intact_p', 'intact_np')
                    
                    GROUP BY b.st_abbrev, c.label 
                    ")


df <- ddply(df, .(st_abbrev),transform, pos = cumsum(acres) - (0.5 * acres))


d = ggplot(df, aes(fill=factor(label, levels=c("non_intact_p",
                                               "non_intact_np",
                                               "nodata_p",
                                               "nodata_np",
                                               "shrubland_p",
                                               "shrubland_np",
                                               "wetland_p",
                                               "wetland_np",
                                               "forest_p",
                                               "forest_np",
                                               "grassland_p",
                                               "grassland_np")), y=acres, x=reorder(st_abbrev, -acres))) + 
  geom_bar(stat="identity", width = 0.2)+
  scale_fill_manual(values = c("forest_p" = "#6c9e77",
                               "forest_np" = "#256e36", 
                               "shrubland_p" = "#ba736e", 
                               "shrubland_np" = "#9c140a", 
                               "wetland_p" = "#708ec2",
                               "wetland_np" = "#023c9f",
                               "grassland_p" = "#b0c27c",
                               "grassland_np" = "#a9d91a",
                               "nodata_p" = "#D1C0DF",
                               "nodata_np" = "#663096",
                               "non_intact_p" = "#696969",
                               "non_intact_np" = "#000000"),
                  labels = '') +

                    # labels = c('f','','f','','f','','f','','f','','f','')) +  
                    
         
  # geom_text(aes(label = acres_scale), position = position_stack(vjust = 0.5)) +
  scale_x_discrete(name="") +
  scale_y_continuous(expand = c(0.1,0.1), labels=formatAC,"millions of acres") +
  guides(fill=guide_legend(title="class",  title.theme = element_text(
    colour = "#4e4d47", 
    size=15,
    vjust=0.0,
    angle = 0
  ))) + 
theme(  axis.ticks = element_blank(),
        axis.line = element_blank(),
        axis.text = element_text(size=12, colour = "#4e4d47"),
        axis.title.x = element_text(colour = "#4e4d47", size=15),
        axis.title.y = element_text(colour = "#4e4d47", size=15, angle=90),
        panel.background = element_rect(fill="#E8E8E8"),
        panel.grid.major.x = element_line(size=1.0),
        panel.grid.minor.x = element_line(colour = "white"),
        panel.grid.major = element_line(colour = "white"),
        legend.spacing.x = unit('0.01','cm'),
        legend.text = element_text(colour='white', size=0,  angle=0, hjust=-0.0, vjust=0.4, margin=margin(0,0,0,0)),
        plot.caption = element_text(size= 15, vjust=8, hjust=-0.09, color = "#4e4d47"), ###title size/position/color
        ####can use justification with the bottom legend postion! (only need one argument then becasue vertical is fixed!!!)
        legend.justification = c(-0.03,-6),
        legend.position="bottom",
        plot.background = element_rect(fill="white"))
d + coord_flip() +
  # labs(title = '?????????????????',
  labs(caption = '        grassland        forest        wetland       shrubland      no-data      non-intact') + 
  guides(fill=guide_legend(nrow=1,byrow=TRUE,title='Broad classes (light classes = protected)',title.theme = element_text(
    size = 15,
    color = "#4e4d47",
    vjust=0.0,
    angle = 0),
    title.position = 'top',label.position = "bottom", reverse = T,  keyheight = unit(4, units = "mm"),keywidth = unit(15, units = "mm")))                                           # legend bin dimensions
                        
                                  

# d  + guides(fill=guide_legend(title='',label.position = "right", reverse = F,  keyheight = unit(6, units = "mm"),keywidth = unit(6, units = "mm")),                                             # legend bin dimensions
#                           keyheight = unit(3, units = "mm"),
#                           keywidth = unit(20, units = "mm"))

fileout = 'D:\\intactland\\graphics\\barchart_cdl_stacked.png'
ggsave(fileout, width = 11, height = 6.5, dpi = 500)


####################################################################################
###############old legend ##########################################################
####################################################################################


# library(ggplot2)
# library(maps)
# library(rgdal)# R wrapper around GDAL/OGR
# library(sp)
# require("RPostgreSQL")
# library(plyr)
# library(dplyr)
# library(viridis)
# library(scales)
# library(rjson)
# # library(jsonlite)
# require(RColorBrewer)
# library(glue)
# 
# 
# 
# drv <- dbDriver("PostgreSQL")
# 
# con <- dbConnect(drv, dbname = "intactland",
#                  host = "144.92.235.105", port = 5432,
#                  user = "mbougie", password = "Mend0ta!")
# 
# 
# 
# ###NOTE: ALWYS CHECK THIS!!!!! ###############################
# million = 10^6
# hundered_thousand = 10^5
# 
# formatAC<-function(x){x/million}  
# 
# 
# 
# 
# 
# ##### query the data from postgreSQL
# df <- dbGetQuery(con,"
#                  -----------------------------------------------------
#                  ----intact lands (ALL lands protected and non-protected)
#                  -----------------------------------------------------
#                  SELECT 
#                  b.st_abbrev, 
#                  a.acres::integer as acres,
#                  c.label
#                  -----------------------------------------------------
#                  FROM 
#                  intact_clu.intactland_15_refined_cdl15_broad_pad_hist_states as a 
#                  
#                  INNER JOIN
#                  spatial.states as b
#                  USING(atlas_st)
#                  
#                  INNER JOIN
#                  intact_clu.intactland_15_refined_cdl15_broad_pad as c
#                  ON a.value::integer = c.value
#                  
#                  
#                  UNION
#                  -------------------------------------------------------------------
#                  ----NON-intact lands (ALL lands protected and non-protected)
#                  -------------------------------------------------------------------
#                  SELECT 
#                  b.st_abbrev, 
#                  sum(a.acres::integer) as acres,
#                  c.label
#                  
#                  FROM 
#                  intact_clu.combined_hist_states as a 
#                  INNER JOIN
#                  spatial.states as b
#                  USING(atlas_st)
#                  INNER JOIN
#                  intact_clu.combined as c
#                  ON a.value::integer = c.value
#                  
#                  ---don't want to work with intact lands
#                  WHERE c.label not in ('intact_p', 'intact_np')
#                  
#                  GROUP BY b.st_abbrev, c.label 
#                  ")
# 
# 
# df <- ddply(df, .(st_abbrev),transform, pos = cumsum(acres) - (0.5 * acres))
# 
# 
# d = ggplot(df, aes(fill=factor(label, levels=c("non_intact_p",
#                                                "non_intact_np",
#                                                "nodata_p",
#                                                "nodata_np",
#                                                "shrubland_p",
#                                                "shrubland_np",
#                                                "wetland_p",
#                                                "wetland_np",
#                                                "forest_p",
#                                                "forest_np",
#                                                "grassland_p",
#                                                "grassland_np")), y=acres, x=reorder(st_abbrev, -acres))) + 
#   geom_bar(stat="identity", width = 0.2)+
#   scale_fill_manual(values = c("forest_p" = "#6c9e77",
#                                "forest_np" = "#256e36", 
#                                "shrubland_p" = "#ba736e", 
#                                "shrubland_np" = "#9c140a", 
#                                "wetland_p" = "#708ec2",
#                                "wetland_np" = "#023c9f",
#                                "grassland_p" = "#b0c27c",
#                                "grassland_np" = "#a9d91a",
#                                "nodata_p" = "#D1C0DF",
#                                "nodata_np" = "#663096",
#                                "non_intact_p" = "#696969",
#                                "non_intact_np" = "#000000"),
#                     labels = c('','non-intact','','no-data    ','','shrubland ','','wetland   ','','forest       ','','grassland ')) +
#   # labels = c('f','','f','','f','','f','','f','','f','')) +  
#   
#   
#   # geom_text(aes(label = acres_scale), position = position_stack(vjust = 0.5)) +
#   scale_x_discrete(name="") +
#   scale_y_continuous(expand = c(0.1,0.1), labels=formatAC,"millions of acres") +
#   guides(fill=guide_legend(title="class",  title.theme = element_text(
#     colour = "#707070", 
#     size=15,
#     vjust=0.0,
#     angle = 0
#   ))) + 
#   theme(  axis.ticks = element_blank(),
#           axis.line = element_blank(),
#           axis.text = element_text(colour = "#707070"),
#           axis.title.x = element_text(colour = "#707070", size=17),
#           axis.title.y = element_text(colour = "#707070", angle=90),
#           panel.background = element_rect(fill="#E8E8E8"),
#           panel.grid.major.x = element_line(size=1.25),
#           panel.grid.minor.x = element_line(colour = "white"),
#           panel.grid.major = element_line(colour = "white"),
#           legend.spacing.x = unit('0.01','cm'),
#           legend.text = element_text(colour="#707070", size = 14,  angle=0, hjust=-0.0, vjust=0.4, margin=margin(0,0,0,0)),
#           # legend.position="right",
#           ####can use justification with the bottom legend postion! (only need one argument then becasue vertical is fixed!!!)
#           legend.justification = c(-0.03,-6),
#           legend.position="bottom",
#           plot.background = element_rect(fill="white"))
# d + coord_flip() +
#   guides(fill=guide_legend(nrow=1,byrow=TRUE,title='Broad classes (light classes represents protected areas)',title.theme = element_text(
#     size = 15,
#     color = "#4e4d47",
#     vjust=0.0,
#     angle = 0),
#     title.position = 'top',label.position = "bottom", reverse = T,  keyheight = unit(4, units = "mm"),keywidth = unit(5, units = "mm")))                                           # legend bin dimensions
# 
# 
# 
# # d  + guides(fill=guide_legend(title='',label.position = "right", reverse = F,  keyheight = unit(6, units = "mm"),keywidth = unit(6, units = "mm")),                                             # legend bin dimensions
# #                           keyheight = unit(3, units = "mm"),
# #                           keywidth = unit(20, units = "mm"))
# 
# fileout = 'D:\\intactland\\graphics\\barchart_cdl_stacked.png'
# ggsave(fileout, width = 11, height = 6.5, dpi = 500)



