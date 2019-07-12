
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

con <- dbConnect(drv, dbname = "intact_lands",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")


formatAC<-function(x){x/100000}  

##### query the data from postgreSQL
df <- dbGetQuery(con, "SELECT atlas_name, acres::integer as acres,(acres::integer)/100000 as acres_scale, landcover FROM intact_clu.intact_lands_cdl")


df <- ddply(df, .(atlas_name),transform, pos = cumsum(acres) - (0.5 * acres))

# ggplot(df, aes(fill=landcover, y=acres, x=atlas_name)) + 
# ggplot(df, aes(fill=landcover, y=acres, x=reorder(atlas_name, -acres))) + 
ggplot(df, aes(fill=factor(landcover, levels=c("other","disturbed","wetland","shrubland","forest","grassland")), y=acres, x=reorder(atlas_name, -acres))) + 
  geom_bar(stat="identity")+
  scale_fill_manual(values = c("forest" = "#256e36", "shrubland" = "#9c140a", "wetland" = "#023c9f", "grassland" = "#a9d91a", "disturbed" = "black", "other"="purple")) + 
  geom_text(aes(label = acres_scale), position = position_stack(vjust = 0.5))+
  # geom_text(aes(label=ifelse(percent >= 0.07, paste0(sprintf("%.0f", percent*100),"%"),"")),position=position_stack(vjust=0.5), colour="white") +               
  scale_y_continuous(
    labels=formatAC,
    "millions of acres", 
    sec.axis = sec_axis(~ . * 1.20, name = "", labels=formatAC)
  )

