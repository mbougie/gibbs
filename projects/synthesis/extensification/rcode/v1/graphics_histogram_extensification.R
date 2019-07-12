
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

# con <- dbConnect(drv, dbname = "usxp_deliverables",
#                  host = "144.92.235.105", port = 5432,
#                  user = "mbougie", password = "Mend0ta!")


##### query the data from postgreSQL
# filename='km2_change_total_sedyield'
# df <- dbGetQuery(con, gsub("current_field", filename, "SELECT atlas_stco, km2_change_total_sedyield as percent FROM synthesis_intensification.rfs_intensification_results_counties WHERE current_field IS NOT NULL;"))
# df <- dbGetQuery(con, "SELECT perc_abandon_rfs_mlra FROM synthesis_extensification.extensification_mlra;")
df <- dbGetQuery(con, "SELECT gal_et_exp_imp_rfs FROM synthesis_extensification.agroibis;")


summary(df)

colMax <- function(data) sapply(data, max, na.rm = TRUE)
colMax(df)
 
# Basic histogram
# p <- ggplot(df, aes(x=gal_irr_exp_imp_rfs)) + geom_histogram()
# # Change the width of bins
# p <- ggplot(df, aes(x=gal_irr_exp_imp_rfs)) + 
#      geom_histogram(binwidth=30)


# # Change colors
# p<-ggplot(df, aes(x=hectares_change_rot_co_napp)) + 
#   geom_histogram(binwidth=5, color="black", fill="white")

p <- ggplot(df, aes(x=gal_et_exp_imp_rfs)) +
  geom_density()

p


