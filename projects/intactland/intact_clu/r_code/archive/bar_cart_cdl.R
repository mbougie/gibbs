
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


formatAC<-function(x){x/1000000}  

##### query the data from postgreSQL
df <- dbGetQuery(con, "SELECT * FROM intact_lands.intact_lands_cdl")


# ggplot(df, aes(fill=landcover, y=acres, x=atlas_name)) + 
ggplot(df, aes(fill=landcover, y=acres, x=reorder(atlas_name, -acres), group=-acres)) + 
  geom_bar(position="dodge", stat="identity")+
  scale_fill_manual(values = c("forest" = "#256e36", "shrubland" = "#9c140a", "wetland" = "#023c9f", "grassland" = "#a9d91a")) + 
  scale_y_continuous(
    labels=formatAC,
    "millions of acres", 
    sec.axis = sec_axis(~ . * 1.20, name = "", labels=formatAC)
  )

