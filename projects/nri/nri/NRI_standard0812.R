library(data.table)
library(maptools)#R package with useful map tools
library(rgeos)#Geomegry Engine Open Source (GEOS)
library(rgdal)#Geospatial Data Analysis Library (GDAL)
library(ggplot2)
library(extrafont)
library(RColorBrewer)
library(plotly)
library(dplyr)
#Getting rid of anything saved in your workspace 
rm(list=ls())

conversionP<-read.csv("G:/GLUE_Work/Projects/NRI_Update/standard_result_net2.csv")
conversion2P<-read.csv("G:/GLUE_Work/Projects/NRI_Update/standard_result_gross2.csv")
conversion3P<-read.csv("G:/GLUE_Work/Projects/NRI_Update/standard_result_gross_compare2.csv")
#conversion3P2<-read.csv("G:/GLUE_Work/Projects/NRI_Update/standard_result_gross_compare2us.csv")
conversion4P<-read.csv("G:/GLUE_Work/Projects/NRI_Update/standard_result_Rgross2.csv")
conversion5P<-read.csv("G:/GLUE_Work/Projects/NRI_Update/standard_result_Rgross_compare2.csv")
#flow
#plot(counties)
#head(counties@data)

# shapefile_name<-"NetChangeCropland_1"
# indir<-"I:/GLUE_Work/Projects/NRI_New/" #Defining the directory that contains the shapefile you are interested in
# map<-readOGR(paste0(indir,shapefile_name,".shp"),layer=shapefile_name)
# #See if the spatial geometry of the shapefile is valid (topological rule check)
# validity<-data.table(valid=gIsValid(map,byid = TRUE))
# sum(validity$valid==F)
# summary(map)#check data attributes, projection, bounding box
# plot(map,main="Map")

#Generate bivariate small multiple line graphs
formatAC<-function(x){x/1000000}
color2011<-ifelse(conversionP$Year==2011,"steelblue3","black")

plotResult<-ggplot(conversionP, aes(x=Year, group=State)) +
  geom_line(aes(y=Conv,color="Conversion"),alpha=0.8)+
  geom_line(aes(y=ConvR*50000,color="Conversion Rate"),alpha=0.8)+
  geom_vline(xintercept = 2011,color="steelblue3")+
  scale_y_continuous(sec.axis = sec_axis(~./50000,name="Percentage of National Contribution"),labels=formatAC)+
  scale_x_continuous(breaks=c(2008,2009,2010,2011,2012))+
  scale_color_manual(values = c("tomato","turquoise"))+
  labs(y="Acreage in Million",x="Years",color="none",caption="The U.S. conversion values were divided by 10 to maintain scale consistency.")+
  facet_wrap(~ State)+
  ggtitle("NRI Net Cropland Conversion 2008-2012")+
  theme(axis.ticks = element_blank(),
        axis.text.x = element_text(size=5,angle=90,color=color2011),
        axis.text.y = element_text(vjust=0),
        axis.title.y=element_text(color="tomato"),
        axis.title.y.right =element_text(color="turquoise"),
        panel.grid.minor.x=element_blank(),
        #panel.background = element_rect(size = 5),
        plot.title = element_text(hjust = 0.5),
        plot.caption = element_text(hjust = 0.5),
        legend.position ="none"
  )
pdf("G:/GLUE_Work/Projects/NRI_Update/plotResult_bivariateNRI_net.pdf")
print(plotResult)
dev.off()


#Generate small mutiple line graphs for cropland conversion rate only
plotResult2<-ggplot(conversionP, aes(x=Year, y=ConvR, group=State)) +
  geom_line(color="turquoise")+
  facet_wrap(~ State)+
  scale_x_continuous(breaks=c(2008,2009,2010,2011,2012))+
  labs(y="Percentage of National Contribution",x="Years",color="none")+
  ggtitle("NRI Net Cropland Conversion 2008-2012")+
  theme(axis.ticks = element_blank(),
        axis.text.x = element_text(size=6,angle=90),
        axis.title.y =element_text(color="turquoise"),
        panel.grid.minor.x=element_blank(),
        plot.title = element_text(hjust = 0.5),
        legend.position = "none"
  )
pdf("G:/GLUE_Work/Projects/NRI_Update/plotResult_rateNRI_net.pdf")
print(plotResult2)
dev.off()


#Generate small mutiple line graphs for cropland conversion (in million acres) only
plotResult3<-ggplot(conversionP, aes(x=Year, y=Conv, group=State)) +
  geom_line(color="tomato")+
  facet_wrap(~ State)+
  scale_y_continuous(labels=formatAC)+
  scale_x_continuous(breaks=c(2008,2009,2010,2011,2012))+
  labs(y="Acreage in Million",x="Years",color="none",caption="The U.S. conversion values were divided by 10 to maintain scale consistency.")+
  ggtitle("NRI Net Cropland Conversion 2008-2012")+
  theme(axis.ticks = element_blank(),
        axis.text.x = element_text(size=6,angle=90),
        axis.title.y=element_text(color="tomato"),
        panel.grid.minor.x=element_blank(),
        plot.title = element_text(hjust = 0.5),
        plot.caption = element_text(hjust = 0.5),
        legend.position = "none"
  )
pdf("G:/GLUE_Work/Projects/NRI_Update/plotResult_acreageNRI_net.pdf")
print(plotResult3)
dev.off()


#Generate bivariate small multiple line graphs
formatAC<-function(x){x/1000000}
color20112<-ifelse(conversion2P$Year==2011,"steelblue3","black")

plotResult4<-ggplot(conversion2P, aes(x=Year, group=State)) +
  geom_line(aes(y=Conv,color="Conversion"),alpha=0.8)+
  geom_line(aes(y=ConvR*50000,color="Conversion Rate"),alpha=0.8)+
  geom_vline(xintercept = 2011,color="steelblue3")+
  scale_y_continuous(sec.axis = sec_axis(~./50000,name="Percentage of National Contribution"),labels=formatAC)+
  scale_x_continuous(breaks=c(2008,2009,2010,2011,2012))+
  scale_color_manual(values = c("tomato","turquoise"))+
  labs(y="Acreage in Million",x="Years",color="none",caption="The U.S. conversion values were divided by 10 to maintain scale consistency.")+
  facet_wrap(~ State)+
  ggtitle("NRI Gross Cropland Conversion 2008-2012")+
  theme(axis.ticks = element_blank(),
        axis.text.x = element_text(size=5,angle=90,color=color20112),
        axis.text.y = element_text(vjust=0),
        axis.title.y=element_text(color="tomato"),
        axis.title.y.right =element_text(color="turquoise"),
        panel.grid.minor.x=element_blank(),
        #panel.background = element_rect(size = 5),
        plot.title = element_text(hjust = 0.5),
        plot.caption = element_text(hjust = 0.5),
        legend.position ="none"
  )
pdf("G:/GLUE_Work/Projects/NRI_Update/plotResult_bivariateNRI_gross.pdf")
print(plotResult4)
dev.off()


#Generate small mutiple line graphs for cropland conversion rate only
plotResult5<-ggplot(conversion2P, aes(x=Year, y=ConvR, group=State)) +
  geom_line(color="turquoise")+
  facet_wrap(~ State)+
  scale_x_continuous(breaks=c(2008,2009,2010,2011,2012))+
  labs(y="Percentage of National Contribution",x="Years",color="none")+
  ggtitle("NRI Gross Cropland Conversion 2008-2012")+
  theme(axis.ticks = element_blank(),
        axis.text.x = element_text(size=6,angle=90),
        axis.title.y =element_text(color="turquoise"),
        panel.grid.minor.x=element_blank(),
        plot.title = element_text(hjust = 0.5),
        legend.position = "none"
  )
pdf("G:/GLUE_Work/Projects/NRI_Update/plotResult_rateNRI_gross.pdf")
print(plotResult5)
dev.off()


#Generate small mutiple line graphs for cropland conversion (in million acres) only
plotResult6<-ggplot(conversion2P, aes(x=Year, y=Conv, group=State)) +
  geom_line(color="tomato")+
  facet_wrap(~ State)+
  scale_y_continuous(labels=formatAC)+
  scale_x_continuous(breaks=c(2008,2009,2010,2011,2012))+
  labs(y="Acreage in Million",x="Years",color="none",caption="The U.S. conversion values were divided by 10 to maintain scale consistency.")+
  ggtitle("NRI Gross Cropland Conversion 2008-2012")+
  theme(axis.ticks = element_blank(),
        axis.text.x = element_text(size=6,angle=90),
        axis.title.y=element_text(color="tomato"),
        panel.grid.minor.x=element_blank(),
        plot.title = element_text(hjust = 0.5),
        plot.caption = element_text(hjust = 0.5),
        legend.position = "none"
  )
pdf("G:/GLUE_Work/Projects/NRI_Update/plotResult_acreageNRI_gross.pdf")
print(plotResult6)
dev.off()

# #Generate bivariate small multiple line graphs
# formatAC<-function(x){x/1000000}
# color201132<-ifelse(conversion3P2$Year==2011,"steelblue3","black")
# 
# plotResult72<-ggplot(conversion3P2, aes(x=Year, group=State)) +
#   geom_line(aes(y=Conv,color="Gross Conversion NRI"),alpha=0.8)+
#   geom_line(aes(y=ConvAC*1,color="Gross Conversion USXP"),alpha=0.8)+
#   geom_vline(xintercept = 2011,color="steelblue3")+
#   scale_y_continuous(sec.axis = sec_axis(~./1,name="Gross Conversion USXP (ac in mil)",labels =formatAC),labels =formatAC)+
#   scale_x_continuous(breaks=c(2009,2010,2011,2012))+
#   scale_color_manual(values = c("tomato","turquoise"))+
#   labs(y="Gross Conversion NRI (ac in mil)",x="Years",color="none")+
#   facet_wrap(~ State)+
#   ggtitle("Comparison for Gross Cropland Conversion between NRI and USXP, 2009-2012")+
#   theme(axis.ticks = element_blank(),
#         axis.text.x = element_text(size=5,angle=90,color=color201132),
#         axis.text.y = element_text(vjust=0),
#         axis.title.y=element_text(color="tomato"),
#         axis.title.y.right =element_text(color="turquoise"),
#         panel.grid.minor.x=element_blank(),
#         #panel.background = element_rect(size = 5),
#         plot.title = element_text(hjust = 0.5),
#         legend.position ="none"
#   )
# pdf("G:/GLUE_Work/Projects/NRI_Update/plotResult_bivariateNRI_gross_compareUS.pdf")
# print(plotResult72)
# dev.off()

#Generate bivariate small multiple line graphs
formatAC<-function(x){x/1000000}
color20113<-ifelse(conversion3P$Year==2011,"steelblue3","black")

plotResult7<-ggplot(conversion3P, aes(x=Year, group=State)) +
  geom_line(aes(y=Conv,color="Gross Conversion NRI"),alpha=0.8)+
  geom_line(aes(y=ConvAC*1,color="Gross Conversion USXP"),alpha=0.8)+
  geom_vline(xintercept = 2011,color="steelblue3",linetype="longdash")+
  scale_y_continuous(sec.axis = sec_axis(~./1,name="Gross Conversion USXP (ac in mil)",labels =formatAC),labels =formatAC)+
  scale_x_continuous(breaks=c(2009,2010,2011,2012))+
  scale_color_manual(values = c("tomato","turquoise"))+
  labs(y="Gross Conversion NRI (ac in mil)",x="Years",color="none",caption="The U.S. conversion values were divided by 10 to maintain scale consistency.")+
  facet_wrap(~ State)+
  ggtitle("Comparison for Gross Cropland Conversion between NRI and USXP, 2009-2012")+
  theme(axis.ticks = element_blank(),
        axis.text.x = element_text(size=5,angle=90,color=color20113),
        axis.text.y = element_text(vjust=0),
        axis.title.y=element_text(color="tomato"),
        axis.title.y.right =element_text(color="turquoise"),
        panel.grid.minor.x=element_blank(),
        #panel.background = element_rect(size = 5),
        plot.title = element_text(hjust = 0.5),
        plot.caption = element_text(hjust = 0.5),
        legend.position ="none"
  )
pdf("G:/GLUE_Work/Projects/NRI_Update/plotResult_bivariateNRI_gross_compare.pdf")
print(plotResult7)
dev.off()


#Generate bivariate small multiple line graphs
formatAC<-function(x){x/1000000}
color20114<-ifelse(conversion4P$Year==2011,"steelblue3","black")

plotResult8<-ggplot(conversion4P, aes(x=Year, group=State)) +
  geom_line(aes(y=Conv,color="Conversion"),alpha=0.8)+
  geom_line(aes(y=ConvR*50000,color="Conversion Rate"),alpha=0.8)+
  geom_vline(xintercept = 2011,color="steelblue3")+
  scale_y_continuous(sec.axis = sec_axis(~./50000,name="Percentage of National Contribution"),labels=formatAC)+
  scale_x_continuous(breaks=c(2008,2009,2010,2011,2012))+
  scale_color_manual(values = c("tomato","turquoise"))+
  labs(y="Acreage in Million",x="Years",color="none",caption="The U.S. conversion values were divided by 10 to maintain scale consistency.")+
  facet_wrap(~ State)+
  ggtitle("NRI Reverse Gross Cropland Conversion 2008-2012")+
  theme(axis.ticks = element_blank(),
        axis.text.x = element_text(size=5,angle=90,color=color20114),
        axis.text.y = element_text(vjust=0),
        axis.title.y=element_text(color="tomato"),
        axis.title.y.right =element_text(color="turquoise"),
        panel.grid.minor.x=element_blank(),
        #panel.background = element_rect(size = 5),
        plot.title = element_text(hjust = 0.5),
        plot.caption = element_text(hjust = 0.5),
        legend.position ="none"
  )
pdf("G:/GLUE_Work/Projects/NRI_Update/plotResult_bivariateNRI_Rgross.pdf")
print(plotResult8)
dev.off()


#Generate small mutiple line graphs for cropland conversion rate only
plotResult9<-ggplot(conversion4P, aes(x=Year, y=ConvR, group=State)) +
  geom_line(color="turquoise")+
  facet_wrap(~ State)+
  scale_x_continuous(breaks=c(2008,2009,2010,2011,2012))+
  labs(y="Percentage of National Contribution",x="Years",color="none")+
  ggtitle("NRI Reverse Gross Cropland Conversion 2008-2012")+
  theme(axis.ticks = element_blank(),
        axis.text.x = element_text(size=6,angle=90),
        axis.title.y =element_text(color="turquoise"),
        panel.grid.minor.x=element_blank(),
        plot.title = element_text(hjust = 0.5),
        legend.position = "none"
  )
pdf("G:/GLUE_Work/Projects/NRI_Update/plotResult_rateNRI_Rgross.pdf")
print(plotResult9)
dev.off()


#Generate small mutiple line graphs for cropland conversion (in million acres) only
plotResult10<-ggplot(conversion4P, aes(x=Year, y=Conv, group=State)) +
  geom_line(color="tomato")+
  facet_wrap(~ State)+
  scale_y_continuous(labels=formatAC)+
  scale_x_continuous(breaks=c(2008,2009,2010,2011,2012))+
  labs(y="Acreage in Million",x="Years",color="none",caption="The U.S. conversion values were divided by 10 to maintain scale consistency.")+
  ggtitle("NRI Reverse Gross Cropland Conversion 2008-2012")+
  theme(axis.ticks = element_blank(),
        axis.text.x = element_text(size=6,angle=90),
        axis.title.y=element_text(color="tomato"),
        panel.grid.minor.x=element_blank(),
        plot.title = element_text(hjust = 0.5),
        plot.caption = element_text(hjust = 0.5),
        legend.position = "none"
  )
pdf("G:/GLUE_Work/Projects/NRI_Update/plotResult_acreageNRI_Rgross.pdf")
print(plotResult10)
dev.off()


# #Generate bivariate small multiple line graphs
# formatAC<-function(x){x/1000000}
# color20114<-ifelse(conversion5P$Year==2011,"steelblue3","black")
# 
# plotResult11<-ggplot(conversion5P, aes(x=Year, group=State)) +
#   geom_line(aes(y=Conv,color="Reverse Gross Conversion NRI"),alpha=0.8)+
#   geom_line(aes(y=ConvAC*1,color="Reverse Gross Conversion USXP"),alpha=0.8)+
#   geom_vline(xintercept = 2011,color="steelblue3")+
#   scale_y_continuous(sec.axis = sec_axis(~./1,name="Reverse Gross Conversion USXP (ac in mil)",labels =formatAC),labels =formatAC)+
#   scale_x_continuous(breaks=c(2009,2010,2011,2012))+
#   scale_color_manual(values = c("tomato","turquoise"))+
#   labs(y="Reverse Gross Conversion NRI (ac in mil)",x="Years",color="none")+
#   facet_wrap(~ State)+
#   ggtitle("Comparison for Reverse Gross Cropland Conversion between NRI and USXP, 2009-2012")+
#   theme(axis.ticks = element_blank(),
#         axis.text.x = element_text(size=5,angle=90,color=color20114),
#         axis.text.y = element_text(vjust=0),
#         axis.title.y=element_text(color="tomato"),
#         axis.title.y.right =element_text(color="turquoise"),
#         panel.grid.minor.x=element_blank(),
#         #panel.background = element_rect(size = 5),
#         plot.title = element_text(hjust = 0.5),
#         legend.position ="none"
#   )
# pdf("G:/GLUE_Work/Projects/NRI_Update/plotResult_bivariateNRI_Rgross_compare.pdf")
# print(plotResult11)
# dev.off()