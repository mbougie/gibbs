library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
drv <- dbDriver("PostgreSQL")

# con <- dbConnect(drv, dbname = "usxp",
#                  host = "144.92.235.105", port = 5432,
#                  user = "mbougie", password = "Mend0ta!")
# 


####  important series   ##########################################################################################

###query the data from postgreSQL

df <- dbGetQuery(con, "SELECT * FROM eric.merged_table where count < 400 AND link=3")


h = median(df$count)


ggplot(data=df, aes(df$count)) + 
geom_histogram(binwidth = 0.5) +
geom_vline(aes(xintercept=median(count)),color="blue", linetype="dashed", size=1) + 
geom_text(aes( 0, h, label = h, hjust = -7, vjust=3), size = 3)


