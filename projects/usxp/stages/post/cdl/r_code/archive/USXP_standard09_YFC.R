library(data.table)
library(maptools)#R package with useful map tools
library(rgeos)#Geomegry Engine Open Source (GEOS)
library(rgdal)#Geospatial Data Analysis Library (GDAL)
library(ggplot2)
library(extrafont)
library(RColorBrewer)
library(plotly)
library(dplyr)
library(geofacet)
library(rgdal)# R wrapper around GDAL/OGR
require("RPostgreSQL")
library(dplyr)

#Get rid of anything saved in your workspace 
rm(list=ls())



drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "usxp",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")


#Set up conversion data variable by reading through the standardized data table
# conversionP<-read.csv("G:/GLUE_Work/Projects/USXP0916_yfc/test_result3_yfc_fips.csv")
df <- dbGetQuery(con, "SELECT * FROM counts_yxc.s20_ytc30_2008to2017_mmu5_fc_states")
# conversionP$state_fips
# #Generate bivariate small multiple line graphs
# formatAC<-function(x){x/1000000}
# #color2011<-ifelse(conversionP$year==2011,"steelblue3","black")
# 
# statenames<- list(
#   "01"="AL",
#   "04"="AZ",
#   "05"="AR",
#   "06"="CA",
#   "08"="CO",
#   "09"="CT",
#   "10"="DE",
#   "12"="FL",
#   "13"="GA",
#   "16"="ID",
#   "17"="IL",
#   "18"="IN",
#   "19"="IA",
#   "20"="KS",
#   "21"="KY",
#   "22"="LA",
#   "23"="ME",
#   "24"="MD",
#   "25"="MA",
#   "26"="MI",
#   "27"="MN",
#   "28"="MS",
#   "29"="MO",
#   "30"="MT",
#   "31"="NE",
#   "32"="NV",
#   "33"="NH",
#   "34"="NJ",
#   "35"="NM",
#   "36"="NY",
#   "37"="NC",
#   "38"="ND",
#   "39"="OH",
#   "40"="OK",
#   "41"="OR",
#   "42"="PA",
#   "44"="RI",
#   "45"="SC",
#   "46"="SD",
#   "47"="TN",
#   "48"="TX",
#   "49"="UT",
#   "50"="VT",
#   "51"="VA",
#   "53"="WA",
#   "54"="WV",
#   "55"="WI",
#   "56"="WY",
#   "57"="US"
# )
# 
# state_labeller<-function(variable,value){
#   return(statenames[value])
# }
# 
plotResult<-ggplot(df, aes(x=year, group=label)) +
  geom_line(aes(y=conv_ac,color="Conversion"),alpha=0.8)+
  # # geom_line(aes(y=conv_r*50000,color="Conversion Rate"),alpha=0.8)+
  # #geom_vline(xintercept = 2011,color="steelblue3")+
  # scale_y_continuous(sec.axis = sec_axis(~./50000,name="Percentage of National Contribution"),labels=formatAC)+
  # scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016))+
  # scale_color_manual(values = c("tomato","turquoise"))+
  # labs(y="Acreage in Million",x="Years",color="none",caption="The U.S. conversion values were divided by 10 to maintain scale consistency.")+
  # facet_wrap(~ state_fips, labeller=state_labeller)+
  # #facet_geo(~ st_abbrev, grid = "us_state_contiguous_grid1")+
  # #facet_wrap(~ state_fips + st_abbrev, labeller =)+
  # #facet_wrap(~ state_fips,)+
  # ggtitle("Cropland Abandonment 2009-2016")+
  # theme(axis.ticks = element_blank(),
  #       axis.text.x = element_text(size=6,angle=90),
  #       axis.text.y = element_text(vjust=0),
  #       axis.title.y=element_text(color="tomato"),
  #       axis.title.y.right =element_text(color="turquoise"),
  #       panel.grid.minor.x=element_blank(),
  #       panel.background = element_rect(size = 20),
  #       plot.title = element_text(hjust = 0.5),
  #       plot.caption = element_text(hjust = 0.5),
  #       legend.position ="none"
  # )
# pdf("G:/GLUE_Work/Projects/USXP0916_yfc/plotResult_bivariate_yfc.pdf")
# print(plotResult)
# dev.off()
# 
# 
# #Generate small mutiple line graphs for cropland conversion rate only
# plotResult2<-ggplot(conversionP, aes(x=year, y=conv_r, group=st_abbrev)) +
#   geom_line(color="turquoise")+
#   facet_wrap(~ state_fips)+
#   scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016))+
#   labs(y="Percentage of National Contribution",x="Years",color="none",caption="The U.S. conversion values were divided by 10 to maintain scale consistency.")+
#   ggtitle("Cropland Abandonment 2009-2016")+
#   theme(axis.ticks = element_blank(),
#         axis.text.x = element_text(size=6,angle=90),
#         axis.title.y =element_text(color="turquoise"),
#         panel.grid.minor.x=element_blank(),
#         plot.title = element_text(hjust = 0.5),
#         plot.caption = element_text(hjust = 0.5),
#         legend.position = "none"
#   )
# pdf("G:/GLUE_Work/Projects/USXP0916_yfc/plotResult_rate_yfc.pdf")
# print(plotResult2)
# dev.off()
# 
# 
# #Generate small mutiple line graphs for cropland conversion (in million acres) only
# plotResult3<-ggplot(conversionP, aes(x=year, y=conv_ac, group=st_abbrev)) +
#   geom_line(color="tomato")+
#   facet_wrap(~ state_fips)+
#   scale_y_continuous(labels=formatAC)+
#   scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016)+
#   labs(y="Acreage in Million",x="Years",color="none",caption="The U.S. conversion values were divided by 10 to maintain scale consistency.")+
#   ggtitle("Cropland Abandonment 2009-2016")+
#   theme(axis.ticks = element_blank(),
#         axis.text.x = element_text(size=6,angle=90),
#         axis.title.y=element_text(color="tomato"),
#         panel.grid.minor.x=element_blank(),
#         plot.title = element_text(hjust = 0.5),
#         plot.caption = element_text(hjust = 0.5),
#         legend.position = "none"
#   )
# pdf("G:/GLUE_Work/Projects/USXP0916_yfc/plotResult_acreage_yfc.pdf")
# print(plotResult3)
# dev.off()
