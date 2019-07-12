# library(extrafont)
# extrafont::loadfonts(device="win")
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
library(reshape2)

drv <- dbDriver("PostgreSQL")

# font_import(pattern = 'ARIALN')
# font_import(pattern = 'ARIALNB')
# 
# con2 <- dbConnect(drv, dbname = "intact_lands",
#                   host = "144.92.235.105", port = 5432,
#                   user = "mbougie", password = "Mend0ta!")


df <- dbGetQuery(con2,"SELECT atlas_stco as county,ytc_2012,ytc_2013,ytc_2014,ytc_2015 FROM intact_clu.test_v3 WHERE state = 'North Dakota'")
df <- melt(df, id=c("county"))

df <- dbGetQuery(con2,"SELECT state,ytc_2012,ytc_2013,ytc_2014,ytc_2015 FROM intact_clu.test_v3_state")
df <- melt(df, id=c("state"))


df&year = as.numeric(gsub('ytc_', '', df$variable), fixed=TRUE)



# #Abandonment
# formatAC<-function(x){x/1000000}
# # color2011<-ifelse(df25$year==2010,"turquoise","black")
# 
# # x$name <- factor(x$name, levels = x$name[order(x$val)])
# 
# ####change the ranking of state based on rank column for BOTH dataframes
# s35_ytc <- mutate(s35_ytc, state = reorder(state, rank))
# print(s35_ytc$state)
# 
# s35_yfc <- mutate(s35_yfc, state = reorder(state, rank))
# print(s35_yfc$state)
# 
# 
# 
# 
# plotResult<-ggplot(NULL, aes(x=variable, group=county)) +
plotResult<-ggplot(NULL, aes(x=variable, group=state)) +
  geom_line(data = df, aes(y=value,color="tomato2"),alpha=0.8)+
  # scale_y_continuous(
  #   labels=formatAC,
  #   "Expansion(acres in mil)",
  #   sec.axis = sec_axis(~ . * 1.20, name = "Abandonment(acres in mil)", labels=formatAC)
  # )+
  # scale_x_continuous(breaks=c(2008,2009,2010,2011,2012,2013,2014,2015,2016,2017))+
  # labs(y="Expansion(acres in mil)",x="Years",color="none",caption="NOTE: CONUS conversion values were divided by 10 to maintain scale consistency.")+
  # facet_wrap(~ county)
  facet_wrap(~ state)
  # ggtitle("Comparison Between Cropland Expansion and Abandonment 2008-2016")+
  # theme(strip.text.x = element_text(size = 7, margin = margin(1,0,1,0, "mm")),
  #       axis.ticks = element_blank(),
  #       axis.text.x = element_text(size=6,angle=90),
  #       axis.text.y = element_text(size=6,vjust=0),
  #       axis.title.y=element_text(color="tomato"),
  #       axis.title.y.right=element_text(color="turquoise"),
  #       panel.grid.minor.x=element_blank(),
  #       #panel.background = element_rect(size = 5),
  #       plot.title = element_text(hjust = 0.5),
  #       plot.caption = element_text(hjust = 0.5),
  #       legend.position ="None"
  # )

plotResult





